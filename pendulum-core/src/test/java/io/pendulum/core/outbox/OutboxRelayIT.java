package io.pendulum.core.outbox;

import io.pendulum.core.retry.BackoffPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** The outbox: recorded transactionally, drained with at-least-once delivery. */
class OutboxRelayIT extends PostgresTestBase {

    private final OutboxStore outbox = new PostgresOutboxStore(DATA_SOURCE);

    @Test
    @DisplayName("a message recorded in a committed transaction is published")
    void committed_messages_are_published() throws Exception {
        ConcurrentLinkedQueue<String> delivered = new ConcurrentLinkedQueue<>();

        UUID id = recordInTransaction(true, "order.created", "{\"orderId\":1}");

        OutboxRelay relay = relayTo(message -> delivered.add(message.destination()));
        assertThat(relay.drainOnce()).isEqualTo(1);

        assertThat(delivered).containsExactly("order.created");
        assertThat(outbox.countInState("PUBLISHED")).isEqualTo(1);
        assertThat(outbox.countInState("PENDING")).isZero();
        assertThat(id).isNotNull();
    }

    /**
     * The half of the pattern people forget. If the business transaction rolls back, the intent
     * must vanish with it — otherwise the outbox happily publishes "order created" for an order
     * that does not exist, which is worse than not publishing at all.
     */
    @Test
    @DisplayName("a rolled-back transaction publishes nothing")
    void rolled_back_messages_never_exist() throws Exception {
        AtomicInteger published = new AtomicInteger();

        recordInTransaction(false, "order.created", "{\"orderId\":2}");

        OutboxRelay relay = relayTo(message -> published.incrementAndGet());
        assertThat(relay.drainOnce()).isZero();

        assertThat(published.get()).isZero();
        assertThat(outbox.countInState("PENDING")).isZero();
        assertThat(outbox.countInState("PUBLISHED")).isZero();
    }

    @Test
    @DisplayName("a failed publish is retried, not lost")
    void failures_are_retried() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        recordInTransaction(true, "flaky.webhook", "{}");

        // Zero backoff so the retry is immediately visible rather than the test sleeping through it.
        OutboxRelay relay = new OutboxRelay(outbox, message -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("webhook returned 503");
            }
        }, Duration.ofMillis(10), Duration.ofMillis(20), 50, BackoffPolicy.fixed(Duration.ZERO));

        assertThat(relay.drainOnce()).isZero();
        assertThat(outbox.countInState("PENDING")).as("still pending after the first failure").isEqualTo(1);

        assertThat(relay.drainOnce()).isZero();
        assertThat(relay.drainOnce()).isEqualTo(1);

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(outbox.countInState("PUBLISHED")).isEqualTo(1);
    }

    @Test
    @DisplayName("a message that never delivers is dead-lettered rather than retried forever")
    void exhausted_messages_are_dead_lettered() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        recordInTransaction(true, "broken.webhook", "{}", 3);

        OutboxRelay relay = new OutboxRelay(outbox, message -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("connection refused");
        }, Duration.ofMillis(10), Duration.ofMillis(20), 50, BackoffPolicy.fixed(Duration.ZERO));

        for (int i = 0; i < 5; i++) {
            relay.drainOnce();
        }

        assertThat(attempts.get()).as("bounded by max attempts").isEqualTo(3);
        assertThat(outbox.countInState("DEAD_LETTERED")).isEqualTo(1);
        assertThat(outbox.countInState("PENDING")).isZero();
    }

    /**
     * The visibility timeout is the outbox's lease. While one relay holds a message, a second must
     * not see it — otherwise every message is delivered twice by construction rather than only in
     * the crash case that at-least-once actually admits.
     */
    @Test
    @DisplayName("a claimed message is invisible to a second relay until its timeout lapses")
    void claiming_hides_the_message_from_other_relays() throws Exception {
        recordInTransaction(true, "order.created", "{}");

        List<OutboxMessage> firstClaim = outbox.claimForPublishing(10, Duration.ofSeconds(30));
        List<OutboxMessage> secondClaim = outbox.claimForPublishing(10, Duration.ofSeconds(30));

        assertThat(firstClaim).hasSize(1);
        assertThat(secondClaim).as("already claimed by the first relay").isEmpty();
    }

    @Test
    @DisplayName("a relay that dies mid-publish loses nothing once the timeout lapses")
    void an_abandoned_claim_becomes_visible_again() throws Exception {
        recordInTransaction(true, "order.created", "{}");

        // Claim with a timeout that has effectively already passed: the relay "died" holding it.
        assertThat(outbox.claimForPublishing(10, Duration.ofMillis(1))).hasSize(1);
        Thread.sleep(50);

        AtomicInteger published = new AtomicInteger();
        OutboxRelay relay = relayTo(message -> published.incrementAndGet());

        assertThat(relay.drainOnce()).isEqualTo(1);
        assertThat(published.get()).isEqualTo(1);
        assertThat(outbox.countInState("PUBLISHED")).isEqualTo(1);
    }

    @Test
    @DisplayName("headers and the dedup key survive the round trip")
    void headers_and_message_key_are_preserved() throws Exception {
        ConcurrentLinkedQueue<OutboxMessage> delivered = new ConcurrentLinkedQueue<>();

        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            outbox.record(connection, OutboxMessage.to("acme", "order.created")
                    .payload("{\"orderId\":7}")
                    .headers(java.util.Map.of("trace-id", "abc123", "source", "checkout"))
                    .messageKey("order-7")
                    .build());
            connection.commit();
        }

        relayTo(delivered::add).drainOnce();

        OutboxMessage message = delivered.peek();
        assertThat(message).isNotNull();
        assertThat(message.messageKey()).isEqualTo("order-7");
        assertThat(message.headers())
                .containsEntry("trace-id", "abc123")
                .containsEntry("source", "checkout");
        assertThat(message.payload()).contains("\"orderId\": 7");
    }

    @Test
    @DisplayName("a relay whose visibility timeout is shorter than its poll interval is rejected")
    void misconfigured_relay_fails_loudly() {
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() ->
                new OutboxRelay(outbox, message -> { },
                        Duration.ofSeconds(5), Duration.ofSeconds(1), 10, BackoffPolicy.fixed(Duration.ZERO))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must exceed pollInterval");
    }

    // ---------------------------------------------------------------- helpers

    private OutboxRelay relayTo(OutboxPublisher publisher) {
        return new OutboxRelay(outbox, publisher,
                Duration.ofMillis(10), Duration.ofSeconds(30), 50, BackoffPolicy.fixed(Duration.ZERO));
    }

    private UUID recordInTransaction(boolean commit, String destination, String payload) throws SQLException {
        return recordInTransaction(commit, destination, payload, 10);
    }

    private UUID recordInTransaction(boolean commit, String destination, String payload, int maxAttempts)
            throws SQLException {
        OutboxMessage message = OutboxMessage.to("acme", destination)
                .payload(payload)
                .maxAttempts(maxAttempts)
                .build();

        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            outbox.record(connection, message);
            if (commit) {
                connection.commit();
            } else {
                connection.rollback();
            }
        }
        return message.id();
    }
}
