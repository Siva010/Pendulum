package io.pendulum.core.store;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lease race, in test form.
 *
 * <p>The scenario every one of these tests circles: worker A's lease expires at T. Worker B picks
 * the job up at T+1ms. Worker A — which was never dead, only slow, because a 400ms GC pause is
 * indistinguishable from a crash to everyone else — finishes at T+2ms and tries to write its
 * result. Timeouts alone cannot prevent that write. A fencing token can, because the write is
 * conditional on still holding the lease that authorised the work.
 */
class FencingIT extends PostgresTestBase {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Test
    @DisplayName("a slow worker cannot complete a job that has been reassigned")
    void stale_completion_is_rejected() {
        UUID id = enqueue("charge-card");

        LeaseToken staleToken = claimOne("worker-a").leaseToken().orElseThrow();

        // Worker A stalls long enough for the lease to lapse and the reaper to take the job back.
        expireLease(id);
        assertThat(store.reapExpiredLeases(10)).isEqualTo(1);

        LeaseToken freshToken = claimOne("worker-b").leaseToken().orElseThrow();
        assertThat(freshToken).isGreaterThan(staleToken);

        // Worker A wakes up and reports success. The fence rejects it.
        assertThat(store.complete(id, staleToken)).isFalse();
        assertThat(stateOf(id)).as("worker B still owns it").isEqualTo("LEASED");

        // Worker B, the rightful owner, is unaffected.
        assertThat(store.complete(id, freshToken)).isTrue();
        assertThat(stateOf(id)).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("a stale worker cannot record a failure either")
    void stale_failure_is_rejected() {
        UUID id = enqueue("charge-card");
        LeaseToken staleToken = claimOne("worker-a").leaseToken().orElseThrow();
        expireLease(id);
        store.reapExpiredLeases(10);
        LeaseToken freshToken = claimOne("worker-b").leaseToken().orElseThrow();

        assertThat(store.retryLater(id, staleToken, Duration.ofSeconds(5), "stale failure")).isFalse();
        assertThat(store.deadLetter(id, staleToken, "stale dead letter")).isFalse();

        assertThat(reload(id).leaseToken()).contains(freshToken);
        assertThat(stateOf(id)).isEqualTo("LEASED");
    }

    @Test
    @DisplayName("heartbeats push the expiry forward and keep the reaper away")
    void heartbeat_renews_the_lease() {
        UUID id = enqueue("long-running-report");
        Job leased = claimOne("worker-a");
        LeaseToken token = leased.leaseToken().orElseThrow();

        expireLease(id);
        assertThat(store.heartbeat(id, token, LEASE)).isTrue();

        // The lease is live again, so the reaper finds nothing to reclaim.
        assertThat(store.reapExpiredLeases(10)).isZero();
        assertThat(stateOf(id)).isEqualTo("LEASED");
    }

    @Test
    @DisplayName("a heartbeat on a lost lease reports the loss instead of silently renewing")
    void heartbeat_with_a_stale_token_fails() {
        UUID id = enqueue("long-running-report");
        LeaseToken staleToken = claimOne("worker-a").leaseToken().orElseThrow();
        expireLease(id);
        store.reapExpiredLeases(10);
        claimOne("worker-b");

        assertThat(store.heartbeat(id, staleToken, LEASE)).isFalse();
    }

    @Test
    @DisplayName("the reaper requeues an orphan and it becomes immediately claimable")
    void reaper_requeues_orphans() {
        UUID id = enqueue("send-email");
        claimOne("worker-a");
        expireLease(id);

        assertThat(store.reapExpiredLeases(10)).isEqualTo(1);

        Job requeued = reload(id);
        assertThat(requeued.state().discriminator()).isEqualTo("PENDING");
        assertThat(requeued.attempt()).as("the crashed attempt is not refunded").isEqualTo(1);
        assertThat(requeued.lastError()).contains("lease expired").contains("worker-a");
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-b", 1, LEASE)).hasSize(1);
    }

    /**
     * The poison-pill guard. A job whose handler reliably kills the JVM never reaches a failure
     * path, so if the reaper requeued unconditionally it would cycle forever, taking a worker down
     * with it each time. Counting the attempt at claim is what makes that terminate.
     */
    @Test
    @DisplayName("an orphan with no attempts left is dead-lettered, not requeued forever")
    void reaper_dead_letters_exhausted_orphans() {
        UUID id = enqueue(NewJob.of("tenant-a", "poison-pill").maxAttempts(2).build());

        for (int attempt = 1; attempt <= 2; attempt++) {
            assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE)).hasSize(1);
            expireLease(id);
            assertThat(store.reapExpiredLeases(10)).isEqualTo(1);
        }

        Job dead = reload(id);
        assertThat(dead.state().discriminator()).isEqualTo("DEAD_LETTERED");
        assertThat(dead.attempt()).isEqualTo(2);
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-b", 1, LEASE)).isEmpty();
    }

    @Test
    @DisplayName("a graceful release refunds the attempt it never used")
    void release_refunds_the_attempt() {
        UUID id = enqueue("send-email");
        LeaseToken token = claimOne("worker-a").leaseToken().orElseThrow();
        assertThat(reload(id).attempt()).isEqualTo(1);

        assertThat(store.release(id, token)).isTrue();

        Job released = reload(id);
        assertThat(released.state().discriminator()).isEqualTo("PENDING");
        assertThat(released.attempt()).as("never started, so it costs nothing").isZero();
    }

    @Test
    @DisplayName("a job that has started cannot be released — we do not know what it did")
    void release_refuses_running_jobs() {
        UUID id = enqueue("charge-card");
        LeaseToken token = claimOne("worker-a").leaseToken().orElseThrow();
        assertThat(store.markRunning(id, token)).isTrue();

        assertThat(store.release(id, token)).isFalse();
        assertThat(stateOf(id)).isEqualTo("RUNNING");
    }

    @Test
    @DisplayName("retryLater schedules against the database clock")
    void retry_later_schedules_from_database_now() {
        UUID id = enqueue("flaky-vendor-call");
        LeaseToken token = claimOne("worker-a").leaseToken().orElseThrow();

        assertThat(store.retryLater(id, token, Duration.ofMinutes(5), "vendor timed out")).isTrue();

        Job retried = reload(id);
        assertThat(retried.state().discriminator()).isEqualTo("PENDING");
        assertThat(retried.lastError()).isEqualTo("vendor timed out");
        assertThat(Duration.between(store.databaseNow(), retried.runAt()))
                .isBetween(Duration.ofMinutes(4), Duration.ofMinutes(6));
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-b", 1, LEASE))
                .as("not due yet").isEmpty();
    }

    private Job claimOne(String workerId) {
        List<Job> claimed = store.claim(NewJob.DEFAULT_QUEUE, workerId, 1, LEASE);
        assertThat(claimed).as("expected %s to claim a job", workerId).hasSize(1);
        return claimed.getFirst();
    }
}
