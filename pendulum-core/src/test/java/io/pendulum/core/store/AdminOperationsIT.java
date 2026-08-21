package io.pendulum.core.store;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Operator surface: listing, replay, and cancellation — with the guards that keep them safe. */
class AdminOperationsIT extends PostgresTestBase {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Test
    @DisplayName("a dead-lettered job can be replayed with a fresh attempt budget")
    void replay_resets_the_attempt_budget() {
        UUID id = deadLetter(NewJob.of("acme", "charge").maxAttempts(2).build());

        assertThat(store.replay(id)).isTrue();

        Job replayed = reload(id);
        assertThat(replayed.state().discriminator()).isEqualTo("PENDING");
        assertThat(replayed.attempt()).as("budget restored").isZero();
        assertThat(replayed.replayCount()).isEqualTo(1);
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE)).hasSize(1);
    }

    /**
     * The count that survives a replay. Without it a job replayed three times looks like a job that
     * has failed once, and the operator loses the single most useful signal that something is
     * systematically broken rather than transiently unlucky.
     */
    @Test
    @DisplayName("replay_count accumulates even though attempts reset")
    void replay_count_is_not_reset() {
        UUID id = deadLetter(NewJob.of("acme", "charge").maxAttempts(1).build());

        store.replay(id);
        deadLetterAgain(id);
        store.replay(id);
        deadLetterAgain(id);
        store.replay(id);

        Job job = reload(id);
        assertThat(job.replayCount()).isEqualTo(3);
        assertThat(job.attempt()).isZero();
    }

    /**
     * The guard that matters most on this whole surface. A "replay" of a job a worker is currently
     * executing is not a replay — it is a second execution racing the first, created by the very
     * operator trying to fix things. Fencing cannot help here, because both executions hold
     * legitimate leases.
     */
    @Test
    @DisplayName("a running job cannot be replayed")
    void replay_refuses_live_jobs() {
        UUID id = enqueue("charge");
        Job leased = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE).getFirst();
        store.markRunning(id, leased.leaseToken().orElseThrow());

        assertThat(store.replay(id)).isFalse();
        assertThat(stateOf(id)).isEqualTo("RUNNING");
    }

    @Test
    @DisplayName("a pending job cannot be replayed either — it is already queued")
    void replay_refuses_pending_jobs() {
        UUID id = enqueue("charge");

        assertThat(store.replay(id)).isFalse();
        assertThat(reload(id).replayCount()).isZero();
    }

    @Test
    @DisplayName("a succeeded job is not replayed by accident")
    void replay_refuses_succeeded_jobs() {
        UUID id = enqueue("charge");
        Job leased = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE).getFirst();
        store.complete(id, leased.leaseToken().orElseThrow());

        assertThat(store.replay(id)).isFalse();
        assertThat(stateOf(id)).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("a pending job can be cancelled and stops being dispatchable")
    void cancel_removes_a_pending_job_from_the_queue() {
        UUID id = enqueue("send-email");

        assertThat(store.cancel(id, "customer withdrew the order")).isTrue();

        Job cancelled = reload(id);
        assertThat(cancelled.state().discriminator()).isEqualTo("CANCELLED");
        assertThat(cancelled.lastError()).contains("customer withdrew");
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 10, LEASE)).isEmpty();
    }

    /**
     * Cancelling in-flight work would be a lie: the handler may already have charged the card. The
     * store refuses, and stopping live work is left to cooperative cancellation through JobContext.
     */
    @Test
    @DisplayName("a leased job cannot be cancelled out from under its worker")
    void cancel_refuses_leased_jobs() {
        UUID id = enqueue("charge");
        store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE);

        assertThat(store.cancel(id, "too late")).isFalse();
        assertThat(stateOf(id)).isEqualTo("LEASED");
    }

    @Test
    @DisplayName("a cancelled job can be put back by replaying it")
    void cancelled_jobs_are_replayable() {
        UUID id = enqueue("send-email");
        store.cancel(id, "cancelled in error");

        assertThat(store.replay(id)).isTrue();
        assertThat(stateOf(id)).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("listing filters by state, tenant and queue")
    void listing_filters() {
        enqueue(NewJob.of("acme", "a").queue("emails").build());
        enqueue(NewJob.of("acme", "b").queue("reports").build());
        enqueue(NewJob.of("globex", "c").queue("emails").build());

        assertThat(store.findJobs(new JobQuery("acme", null, null, null, 50, 0))).hasSize(2);
        assertThat(store.findJobs(new JobQuery(null, "emails", null, null, 50, 0))).hasSize(2);
        assertThat(store.findJobs(new JobQuery("acme", "emails", "PENDING", null, 50, 0))).hasSize(1);
        assertThat(store.countJobs(new JobQuery(null, null, "PENDING", null, 50, 0))).isEqualTo(3);
    }

    @Test
    @DisplayName("listing pages, and the page size is capped")
    void listing_pages_and_caps() {
        for (int i = 0; i < 8; i++) {
            enqueue("bulk");
        }

        List<Job> firstPage = store.findJobs(new JobQuery(null, null, null, null, 5, 0));
        List<Job> secondPage = store.findJobs(new JobQuery(null, null, null, null, 5, 5));

        assertThat(firstPage).hasSize(5);
        assertThat(secondPage).hasSize(3);
        assertThat(firstPage).doesNotContainAnyElementsOf(secondPage);

        // An operator asking for everything gets a page, not the whole table.
        assertThat(new JobQuery(null, null, null, null, 10_000, 0).limit()).isEqualTo(JobQuery.MAX_LIMIT);
    }

    @Test
    @DisplayName("the dead-letter listing shows the failure chain")
    void dead_letter_listing_preserves_the_error() {
        deadLetter(NewJob.of("acme", "charge").maxAttempts(1).build());

        List<Job> deadLetters = store.findJobs(JobQuery.deadLetters(50, 0));

        assertThat(deadLetters).hasSize(1);
        assertThat(deadLetters.getFirst().lastError()).contains("card declined");
    }

    // ---------------------------------------------------------------- helpers

    private UUID deadLetter(NewJob newJob) {
        UUID id = enqueue(newJob);
        deadLetterAgain(id);
        return id;
    }

    private void deadLetterAgain(UUID id) {
        Job leased = store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 1, LEASE).getFirst();
        store.deadLetter(id, leased.leaseToken().orElseThrow(), "card declined");
    }
}
