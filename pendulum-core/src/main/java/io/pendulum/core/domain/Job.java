package io.pendulum.core.domain;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * A job as it exists in the database. Immutable: every transition produces a new row
 * version through a conditional UPDATE, never a mutation of this object.
 *
 * @param payload the payload as JSON text. Deliberately not a deserialized object graph:
 *                Java native serialization of untrusted payloads is one of the most
 *                exploited vulnerability classes on the JVM, so payloads cross the
 *                boundary as JSON and are bound to a type the handler names explicitly.
 */
public record Job(
        UUID id,
        String tenantId,
        String queue,
        String jobType,
        String payload,
        JobState state,
        int priority,
        Instant runAt,
        int attempt,
        int maxAttempts,
        String idempotencyKey,
        String lastError,
        int replayCount,
        Instant createdAt,
        Instant updatedAt
) {

    public Optional<LeaseToken> leaseToken() {
        return state.leaseToken();
    }

    /** True when this attempt is the last one the retry budget allows. */
    public boolean isFinalAttempt() {
        return attempt >= maxAttempts;
    }

    public int attemptsRemaining() {
        return Math.max(0, maxAttempts - attempt);
    }
}
