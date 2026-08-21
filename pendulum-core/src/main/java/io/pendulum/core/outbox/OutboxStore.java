package io.pendulum.core.outbox;

import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Persistence for the outbox. Mirrors {@code JobStore}'s discipline, for the same reasons. */
public interface OutboxStore {

    /**
     * Record the intent inside a transaction the caller owns — this is the entire point of the
     * pattern, so there is deliberately no variant that opens its own connection. An outbox write
     * that does not join the business transaction provides nothing that a plain publish does not,
     * while costing an extra table and a relay.
     */
    void record(Connection connection, OutboxMessage message);

    /**
     * Claim a batch for publishing by pushing each row's visibility timeout forward.
     *
     * <p>{@code next_attempt_at} is the lease: no separate lock column is needed, because a row
     * that is invisible until some future instant is precisely a row someone else is working on.
     * If the relay dies mid-publish, the timeout lapses and another relay takes over — recovery
     * that does not depend on the dead process having done anything.
     */
    List<OutboxMessage> claimForPublishing(int limit, Duration visibilityTimeout);

    /** Mark delivered. */
    boolean markPublished(UUID id);

    /** Delivery failed; make it visible again after {@code delay}. */
    boolean markFailed(UUID id, Duration delay, String error);

    /** Attempts exhausted, or a permanently undeliverable message. */
    boolean markDeadLettered(UUID id, String error);

    long countInState(String state);
}
