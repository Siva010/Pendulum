package io.pendulum.core.engine;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Kill workers mid-execution and prove nothing is lost and nothing is applied twice.
 *
 * <p>{@link Worker#terminateAbruptly()} models {@code kill -9}: the poll loop stops, the heartbeat
 * stops, and — this is the part that matters — the worker writes nothing further. A dead JVM does
 * not get to record a failure or release a lease, so a simulation that let it do either would be
 * testing an orderly shutdown wearing a crash costume.
 *
 * <p>Leases are deliberately short here (two seconds). In production the lease duration is a
 * recovery-time knob: it is the worst case between a worker dying and its work being picked up by
 * someone else.
 */
class ChaosIT extends PostgresTestBase {

    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final Duration HEARTBEAT = Duration.ofMillis(400);

    @Test
    @DisplayName("a job whose worker is killed mid-execution is finished by another worker, once")
    void work_survives_a_killed_worker() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger handlerCompletions = new AtomicInteger();

        // The victim: it starts, signals, and never returns — the worker dies underneath it.
        HandlerRegistry victimHandlers = new HandlerRegistry();
        victimHandlers.register("resumable", context -> {
            started.countDown();
            Thread.sleep(Duration.ofMinutes(5));
        });

        UUID id = enqueue("resumable");

        Worker victim = worker("victim", victimHandlers);
        victim.start();
        assertThat(started.await(20, TimeUnit.SECONDS)).as("handler started").isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> stateOf(id).equals("RUNNING"));

        victim.terminateAbruptly();

        HandlerRegistry survivorHandlers = new HandlerRegistry();
        survivorHandlers.register("resumable", context -> handlerCompletions.incrementAndGet());

        try (LeaseReaper reaper = new LeaseReaper(store, Duration.ofMillis(250), 100);
             Worker survivor = worker("survivor", survivorHandlers)) {
            reaper.start();
            survivor.start();

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(stateOf(id)).isEqualTo("SUCCEEDED"));

            assertThat(handlerCompletions.get()).as("applied exactly once").isEqualTo(1);
            assertThat(reload(id).attempt()).as("the crashed attempt was counted").isEqualTo(2);
        }
    }

    /**
     * Be precise about what this proves, because the imprecise version of this claim is how you
     * lose credibility in an interview.
     *
     * <p>It proves <em>no job is lost</em>: every one of the eighty ends SUCCEEDED, including the
     * ones that were mid-flight in a process that died without warning. It does <em>not</em> prove
     * each handler ran exactly once — it cannot, and no leasing scheme can. A worker that finishes
     * the side effect and dies before recording it leaves the database no way to distinguish that
     * from a worker that died before doing anything, so the job is retried and the handler runs
     * again. That is at-least-once delivery, and it is the honest ceiling.
     *
     * <p>Effectively-once <em>effects</em> come from pairing that with an idempotent handler, which
     * is what {@code applied.add(id)} stands in for here: replays are counted, and the observable
     * outcome is still one effect per job.
     */
    @Test
    @DisplayName("a fleet losing a worker mid-flight loses no jobs; replays stay bounded")
    void a_batch_survives_a_worker_dying_mid_flight() throws Exception {
        int jobCount = 80;
        Set<UUID> applied = ConcurrentHashMap.newKeySet();
        AtomicInteger replays = new AtomicInteger();
        CountDownLatch victimStartedWork = new CountDownLatch(5);

        JobHandler handler = context -> {
            Thread.sleep(30);
            if (!applied.add(context.job().id())) {
                replays.incrementAndGet();
            }
        };

        HandlerRegistry victimHandlers = new HandlerRegistry();
        victimHandlers.register("chaotic", context -> {
            victimStartedWork.countDown();
            handler.handle(context);
        });

        HandlerRegistry survivorHandlers = new HandlerRegistry();
        survivorHandlers.register("chaotic", handler);

        for (int i = 0; i < jobCount; i++) {
            enqueue("chaotic");
        }

        Worker victim = worker("victim", victimHandlers);
        try (LeaseReaper reaper = new LeaseReaper(store, Duration.ofMillis(250), 100);
             Worker survivor = worker("survivor", survivorHandlers)) {
            reaper.start();
            victim.start();
            survivor.start();

            assertThat(victimStartedWork.await(30, TimeUnit.SECONDS)).isTrue();
            victim.terminateAbruptly();

            await().atMost(Duration.ofSeconds(90))
                    .untilAsserted(() -> assertThat(countInState("SUCCEEDED")).isEqualTo(jobCount));
        }

        assertThat(applied).as("every job ran to completion at least once").hasSize(jobCount);
        assertThat(countInState("DEAD_LETTERED")).as("nothing was written off").isZero();
        assertThat(countInState("PENDING")).as("nothing was left behind").isZero();
        // Replays are bounded by what the victim had in flight when it died — not by the queue
        // depth. A design without fencing has no such bound, because a stale worker keeps writing.
        assertThat(replays.get())
                .as("replays are bounded by the victim's in-flight window, not the backlog")
                .isLessThanOrEqualTo(16);
    }

    /**
     * The lease race in its purest form: the worker is not dead, only slow, and it is still
     * holding a handler that is about to finish. By the time it finishes, the job belongs to
     * someone else — and the database, not the worker's good intentions, is what stops it from
     * writing.
     */
    @Test
    @DisplayName("a slow worker notices it lost its lease and never writes a stale result")
    void a_stalled_worker_is_fenced_off() throws Exception {
        CountDownLatch handlerRunning = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("slow", context -> {
            handlerRunning.countDown();
            // Wait for the test to steal the lease out from under us, then "finish" the work.
            releaseHandler.await(30, TimeUnit.SECONDS);
        });

        UUID id = enqueue("slow");

        try (Worker stalled = worker("stalled", handlers)) {
            stalled.start();
            assertThat(handlerRunning.await(20, TimeUnit.SECONDS)).isTrue();

            // Simulate the stall: the lease lapses and another worker takes the job. Done in one
            // statement so the stalled worker's own heartbeat cannot win the race and renew it.
            LeaseToken thiefToken = new LeaseToken(stealLease(id, "thief"));

            // The stalled worker's own heartbeat is what tells it the lease is gone.
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> stalled.metrics().leasesLost() >= 1);

            releaseHandler.countDown();

            // Give the stalled worker every chance to write something it should not.
            Thread.sleep(1000);
            Job job = reload(id);
            assertThat(job.state().discriminator()).as("the thief still owns it").isEqualTo("LEASED");
            assertThat(job.leaseToken()).contains(thiefToken);
            assertThat(stalled.metrics().succeeded()).isZero();
        }
    }

    private Worker worker(String id, HandlerRegistry handlers) {
        return new Worker(store, handlers, RetryPolicy.defaults(),
                WorkerConfig.defaults(NewJob.DEFAULT_QUEUE)
                        .withWorkerId(id)
                        .withLease(LEASE, HEARTBEAT)
                        .withConcurrency(8, 16)
                        .withPollInterval(Duration.ofMillis(25), Duration.ofMillis(200))
                        .withDrainTimeout(Duration.ofSeconds(5)));
    }
}
