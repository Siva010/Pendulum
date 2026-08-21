package io.pendulum.core.domain;

import java.time.Instant;
import java.util.Optional;

/**
 * The job state machine, as a sealed hierarchy rather than an enum.
 *
 * <p>The reason is that the states are not interchangeable labels: a leased job has a
 * fencing token, an owner and an expiry; a dead-lettered job has a failure reason. With
 * an enum those fields live on the job as nullable columns and every read site has to
 * remember which combinations are legal. Here the compiler enforces it, and an
 * exhaustive {@code switch} over this interface fails to compile when a state is added.
 */
public sealed interface JobState {

    /** Waiting to be dispatched. {@code run_at} decides when it becomes eligible. */
    record Pending() implements JobState {}

    /** Claimed by a worker, not yet started. Prefetched work sits here. */
    record Leased(LeaseToken token, String owner, Instant expiresAt) implements JobState {}

    /** A handler is executing. Heartbeats push {@code expiresAt} forward. */
    record Running(LeaseToken token, String owner, Instant expiresAt) implements JobState {}

    /** Terminal success. */
    record Succeeded(Instant at) implements JobState {}

    /** Failed and not currently scheduled — a manual-intervention state, not a retry. */
    record Failed(String error) implements JobState {}

    /** Terminal failure: attempts exhausted, or a non-retryable error class. */
    record DeadLettered(String error, Instant at) implements JobState {}

    /** Terminal by operator decision. Kept as a state rather than a deletion so the record survives. */
    record Cancelled(String reason, Instant at) implements JobState {}

    /** The persisted discriminator for this state. */
    default String discriminator() {
        return switch (this) {
            case Pending ignored      -> "PENDING";
            case Leased ignored       -> "LEASED";
            case Running ignored      -> "RUNNING";
            case Succeeded ignored    -> "SUCCEEDED";
            case Failed ignored       -> "FAILED";
            case DeadLettered ignored -> "DEAD_LETTERED";
            case Cancelled ignored    -> "CANCELLED";
        };
    }

    /** True for states from which no further transition happens without an operator. */
    default boolean isTerminal() {
        // One pattern per case label: Java 21 does not allow multi-pattern labels.
        return switch (this) {
            case Pending p      -> false;
            case Leased l       -> false;
            case Running r      -> false;
            case Succeeded s    -> true;
            case Failed f       -> true;
            case DeadLettered d -> true;
            case Cancelled c    -> true;
        };
    }

    /**
     * True for terminal states an operator may replay. Deliberately excludes the live states: a
     * "replay" of a RUNNING job would not be a replay, it would be a second execution racing the
     * first, which is the precise thing the rest of this engine exists to prevent.
     */
    default boolean isReplayable() {
        return switch (this) {
            case DeadLettered d -> true;
            case Failed f       -> true;
            case Cancelled c    -> true;
            case Succeeded s    -> false;
            case Pending p      -> false;
            case Leased l       -> false;
            case Running r      -> false;
        };
    }

    /** The fencing token held in this state, or empty for states that hold no lease. */
    default Optional<LeaseToken> leaseToken() {
        return switch (this) {
            case Leased l       -> Optional.of(l.token());
            case Running r      -> Optional.of(r.token());
            case Pending p      -> Optional.empty();
            case Succeeded s    -> Optional.empty();
            case Failed f       -> Optional.empty();
            case DeadLettered d -> Optional.empty();
            case Cancelled c    -> Optional.empty();
        };
    }
}
