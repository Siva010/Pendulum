package io.pendulum.core.store;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.JobState;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Dispatch: eligibility, ordering, and the property that makes N pollers safe. */
class LeaseDispatchIT extends PostgresTestBase {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Test
    @DisplayName("a claimed job carries a lease, an owner, and an expiry from the database clock")
    void claim_grants_a_lease() {
        UUID id = enqueue("send-email");

        List<Job> claimed = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 10, LEASE);

        assertThat(claimed).hasSize(1);
        Job job = claimed.getFirst();
        assertThat(job.id()).isEqualTo(id);
        assertThat(job.state()).isInstanceOf(JobState.Leased.class);

        JobState.Leased leased = (JobState.Leased) job.state();
        assertThat(leased.owner()).isEqualTo("worker-a");
        assertThat(leased.expiresAt()).isAfter(store.databaseNow());
        assertThat(leased.token().value()).isPositive();
    }

    @Test
    @DisplayName("jobs scheduled for the future are not eligible")
    void claim_skips_jobs_that_are_not_due() {
        enqueue(NewJob.of("tenant-a", "send-email").runAfter(Duration.ofHours(1)).build());

        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 10, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("a delay is resolved against the database clock, not the JVM clock")
    void relative_schedules_use_the_database_clock() {
        UUID id = enqueue(NewJob.of("tenant-a", "send-email").runAfter(Duration.ofMinutes(10)).build());

        Duration untilDue = Duration.between(store.databaseNow(), reload(id).runAt());

        assertThat(untilDue).isBetween(Duration.ofMinutes(9), Duration.ofMinutes(11));
    }

    @Test
    @DisplayName("higher priority first, then oldest run_at")
    void claim_respects_priority_then_age() {
        UUID low = enqueue(NewJob.of("tenant-a", "batch").priority(0).build());
        UUID high = enqueue(NewJob.of("tenant-a", "urgent").priority(10).build());
        UUID medium = enqueue(NewJob.of("tenant-a", "normal").priority(5).build());

        List<UUID> order = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 10, LEASE)
                .stream().map(Job::id).toList();

        assertThat(order).containsExactly(high, medium, low);
    }

    @Test
    @DisplayName("queues are isolated")
    void claim_only_touches_its_own_queue() {
        enqueue(NewJob.of("tenant-a", "email").queue("emails").build());
        UUID reportJob = enqueue(NewJob.of("tenant-a", "report").queue("reports").build());

        List<Job> claimed = store.claim("reports", "worker-a", 10, LEASE);

        assertThat(claimed).extracting(Job::id).containsExactly(reportJob);
    }

    @Test
    @DisplayName("the attempt counter advances at claim time, not at failure time")
    void claim_consumes_an_attempt() {
        UUID id = enqueue("send-email");
        assertThat(reload(id).attempt()).isZero();

        store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE);

        assertThat(reload(id).attempt()).isEqualTo(1);
    }

    @Test
    @DisplayName("every claim issues a strictly greater fencing token")
    void tokens_are_monotonic() {
        enqueue("a");
        enqueue("b");

        List<Job> claimed = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 2, LEASE);
        List<Long> tokens = claimed.stream()
                .map(job -> job.leaseToken().map(LeaseToken::value).orElseThrow())
                .toList();

        assertThat(tokens).isSorted();
        assertThat(tokens.getFirst()).isLessThan(tokens.getLast());
    }

    /**
     * The property the whole dispatch design exists for. Without {@code SKIP LOCKED} — with a
     * SELECT followed by an UPDATE — this test fails by handing the same job to two workers,
     * which in production is a customer charged twice.
     */
    @Test
    @DisplayName("concurrent workers never claim the same job twice")
    void concurrent_claims_partition_the_queue() throws Exception {
        int jobCount = 600;
        int workerCount = 8;
        for (int i = 0; i < jobCount; i++) {
            enqueue("send-email");
        }

        ConcurrentLinkedQueue<UUID> allClaims = new ConcurrentLinkedQueue<>();
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(workerCount);

        try (ExecutorService workers = Executors.newFixedThreadPool(workerCount)) {
            for (int i = 0; i < workerCount; i++) {
                String workerId = "worker-" + i;
                workers.execute(() -> {
                    try {
                        startGate.await();
                        List<Job> batch;
                        do {
                            batch = store.claim(NewJob.DEFAULT_QUEUE, workerId, 10, LEASE);
                            batch.forEach(job -> allClaims.add(job.id()));
                        } while (!batch.isEmpty());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }
            startGate.countDown();
            assertThat(finished.await(60, TimeUnit.SECONDS)).isTrue();
        }

        List<UUID> claims = new ArrayList<>(allClaims);
        Set<UUID> distinct = new HashSet<>(claims);

        assertThat(claims).as("every job claimed exactly once").hasSize(jobCount);
        assertThat(distinct).as("no job claimed twice").hasSize(jobCount);
        assertThat(countInState("PENDING")).isZero();
    }

    @Test
    @DisplayName("an idempotency key makes enqueue a no-op the second time")
    void enqueue_is_idempotent_under_a_key() {
        NewJob first = NewJob.of("tenant-a", "charge-card").idempotencyKey("order-42").build();
        NewJob second = NewJob.of("tenant-a", "charge-card").idempotencyKey("order-42").build();

        JobStore.EnqueueResult firstResult = store.enqueue(first);
        JobStore.EnqueueResult secondResult = store.enqueue(second);

        assertThat(firstResult.created()).isTrue();
        assertThat(secondResult.created()).isFalse();
        assertThat(secondResult.id()).isEqualTo(firstResult.id());
        assertThat(countInState("PENDING")).isEqualTo(1);
    }

    @Test
    @DisplayName("idempotency keys are scoped per tenant")
    void idempotency_keys_do_not_collide_across_tenants() {
        store.enqueue(NewJob.of("tenant-a", "charge-card").idempotencyKey("order-42").build());
        JobStore.EnqueueResult other =
                store.enqueue(NewJob.of("tenant-b", "charge-card").idempotencyKey("order-42").build());

        assertThat(other.created()).isTrue();
        assertThat(countInState("PENDING")).isEqualTo(2);
    }
}
