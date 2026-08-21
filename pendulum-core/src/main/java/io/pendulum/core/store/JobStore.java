package io.pendulum.core.store;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.domain.NewJob;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The persistence boundary of the engine.
 *
 * <p>Every method that reports the outcome of an execution takes a {@link LeaseToken} and
 * returns {@code boolean}. That signature is the fencing contract: {@code false} means
 * "you no longer own this job — your lease expired and someone else has it". A worker that
 * ignores those booleans is a worker that double-applies effects.
 */
public interface JobStore {

    /** The outcome of an enqueue. {@code created == false} means an idempotency key matched. */
    record EnqueueResult(UUID id, boolean created) {}

    EnqueueResult enqueue(NewJob job);

    /**
     * Enqueue inside a transaction the caller already owns.
     *
     * <p>The single most valuable operation in the engine, and the reason Postgres was chosen over
     * a dedicated queue. The naive alternatives both lose:
     *
     * <pre>{@code
     * // Enqueue after commit: process dies in the gap. Order created, email never sent, silently.
     * orderRepository.save(order);   // committed
     * queue.publish(sendEmailJob);   // <- crash here and the job simply never existed
     *
     * // Enqueue before commit: the transaction rolls back and you have a phantom job for an
     * // order that does not exist.
     * }</pre>
     *
     * With one connection and one transaction, the job and the business write commit or roll back
     * together, and neither outcome is possible. Kafka cannot do this — it cannot join your
     * database transaction — which is exactly the tradeoff the README documents.
     *
     * <p>The implementation must not commit, roll back, or close the connection.
     */
    EnqueueResult enqueue(java.sql.Connection connection, NewJob job);

    /**
     * Atomically claim up to {@code limit} eligible jobs, each with a fresh fencing token.
     *
     * <p>One statement, not select-then-update: two workers running
     * {@code SELECT ... WHERE state='pending'} followed by {@code UPDATE ... SET state='running'}
     * will both see the same row. {@code SKIP LOCKED} makes concurrent pollers step over
     * each other's rows instead of queueing behind them.
     */
    List<Job> claim(String queue, String owner, int limit, Duration leaseDuration);

    /** LEASED -> RUNNING. Fenced. Marks the moment a handler actually started. */
    boolean markRunning(UUID id, LeaseToken token);

    /** Push the lease expiry forward. Fenced — a {@code false} return means the lease was lost. */
    boolean heartbeat(UUID id, LeaseToken token, Duration leaseDuration);

    /** Terminal success. Fenced. */
    boolean complete(UUID id, LeaseToken token);

    /** Back to PENDING, eligible again after {@code delay} measured from database now(). Fenced. */
    boolean retryLater(UUID id, LeaseToken token, Duration delay, String error);

    /** Terminal failure. Fenced. */
    boolean deadLetter(UUID id, LeaseToken token, String error);

    /**
     * Hand a claimed-but-never-started job back during graceful shutdown, refunding the
     * attempt that {@link #claim} consumed. Only legal from LEASED: once a handler has
     * started we cannot know whether side effects happened, so a RUNNING job is left to
     * expire instead.
     */
    boolean release(UUID id, LeaseToken token);

    /**
     * Requeue jobs whose lease expired, dead-lettering those that have no attempts left.
     *
     * @return the number of jobs reclaimed
     */
    int reapExpiredLeases(int limit);

    Optional<Job> find(UUID id);

    /** Filtered, paged listing for the admin surface. */
    List<Job> findJobs(JobQuery query);

    long countJobs(JobQuery query);

    /**
     * Send a terminal job back to the queue with a fresh attempt budget.
     *
     * <p>Only legal from a terminal state — see {@code JobState#isReplayable()}. Replaying a LEASED
     * or RUNNING job would not be a replay, it would be a second execution racing the first, and no
     * amount of fencing helps when the operator is the one creating the duplicate.
     */
    boolean replay(UUID id);

    /**
     * Cancel a job that has not started.
     *
     * <p>Only legal from PENDING. Once a worker holds the lease the handler may already have caused
     * effects, and a store that flipped the row to CANCELLED underneath it would be lying to the
     * operator about what happened. Stopping in-flight work needs cooperative cancellation through
     * {@code JobContext}, which is a different feature.
     */
    boolean cancel(UUID id, String reason);

    /** Job counts by state for one queue — the signal KEDA should autoscale on. */
    Map<String, Long> queueDepth(String queue);

    /** The database clock. The only clock in the system that every participant agrees on. */
    Instant databaseNow();
}
