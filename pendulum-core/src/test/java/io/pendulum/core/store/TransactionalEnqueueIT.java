package io.pendulum.core.store;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The feature that eliminates an entire class of production bug.
 *
 * <p>The bug: you save an order, then enqueue the confirmation email. Between those two statements
 * the process can die — and then the order exists forever with no email, silently, with nothing
 * anywhere recording that anything went wrong. Reverse the order and a rolled-back transaction
 * leaves a job referring to an order that never existed.
 *
 * <p>There is no ordering of two independent systems that fixes this. There is only putting both
 * writes in one transaction, which is available precisely because the queue lives in the same
 * database as the business data — the entire argument for choosing Postgres over a dedicated
 * broker.
 */
class TransactionalEnqueueIT extends PostgresTestBase {

    @BeforeEach
    void createBusinessTable() {
        execute("CREATE TABLE IF NOT EXISTS test_orders (id UUID PRIMARY KEY, customer TEXT NOT NULL)");
        execute("TRUNCATE TABLE test_orders");
    }

    @Test
    @DisplayName("a job enqueued in a committed transaction is visible with the business write")
    void commit_makes_both_visible() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID jobId;

        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            insertOrder(connection, orderId, "acme");
            jobId = store.enqueue(connection, NewJob.of("acme", "send-confirmation")
                    .payload("{\"orderId\":\"" + orderId + "\"}")
                    .build()).id();
            connection.commit();
        }

        assertThat(countOrders()).isEqualTo(1);
        assertThat(stateOf(jobId)).isEqualTo("PENDING");
        assertThat(store.claim(NewJob.DEFAULT_QUEUE, "worker-a", 10, java.time.Duration.ofSeconds(30)))
                .as("and it is dispatchable")
                .hasSize(1);
    }

    @Test
    @DisplayName("a rolled-back transaction leaves no phantom job")
    void rollback_takes_the_job_with_it() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID jobId;

        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            insertOrder(connection, orderId, "acme");
            jobId = store.enqueue(connection, NewJob.of("acme", "send-confirmation").build()).id();
            // The business logic rejects the order after the job was already staged.
            connection.rollback();
        }

        assertThat(countOrders()).as("no order").isZero();
        assertThat(store.find(jobId)).as("and therefore no job").isEmpty();
        assertThat(countInState("PENDING")).isZero();
    }

    /**
     * The failure the pattern exists to prevent, made concrete: the connection dies after the
     * business write but before the transaction commits. Both must vanish together. With a separate
     * broker this is the case that leaves an order with no email and no trace.
     */
    @Test
    @DisplayName("a connection lost mid-transaction loses the order and the job together")
    void a_lost_connection_loses_neither_half_alone() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();

        Connection connection = DATA_SOURCE.getConnection();
        try {
            connection.setAutoCommit(false);
            insertOrder(connection, orderId, "acme");
            store.enqueue(connection, NewJob.of("acme", "send-confirmation").id(jobId).build());
            // No commit. Closing an uncommitted connection is a rollback — the same outcome a
            // crashed process produces.
        } finally {
            connection.close();
        }

        assertThat(countOrders()).isZero();
        assertThat(store.find(jobId)).isEmpty();
    }

    @Test
    @DisplayName("enqueue does not commit or close the caller's connection")
    void the_store_does_not_touch_the_callers_transaction() throws Exception {
        UUID orderId = UUID.randomUUID();

        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            store.enqueue(connection, NewJob.of("acme", "first").build());

            assertThat(connection.isClosed()).as("still open").isFalse();
            assertThat(connection.getAutoCommit()).as("still in a transaction").isFalse();

            // Uncommitted work is invisible to everyone else, which is how we know the store did
            // not quietly commit on our behalf.
            assertThat(countInState("PENDING")).as("not visible from another connection").isZero();

            insertOrder(connection, orderId, "acme");
            store.enqueue(connection, NewJob.of("acme", "second").build());
            connection.commit();
        }

        assertThat(countInState("PENDING")).isEqualTo(2);
        assertThat(countOrders()).isEqualTo(1);
    }

    @Test
    @DisplayName("an idempotency key still deduplicates inside a transaction")
    void idempotency_survives_the_transactional_path() throws Exception {
        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            JobStore.EnqueueResult first = store.enqueue(connection,
                    NewJob.of("acme", "charge").idempotencyKey("order-99").build());
            JobStore.EnqueueResult second = store.enqueue(connection,
                    NewJob.of("acme", "charge").idempotencyKey("order-99").build());
            connection.commit();

            assertThat(first.created()).isTrue();
            assertThat(second.created()).isFalse();
            assertThat(second.id()).isEqualTo(first.id());
        }

        assertThat(countInState("PENDING")).isEqualTo(1);
    }

    private static void insertOrder(Connection connection, UUID id, String customer) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test_orders (id, customer) VALUES (?, ?)")) {
            statement.setObject(1, id);
            statement.setString(2, customer);
            statement.executeUpdate();
        }
    }

    private long countOrders() {
        try (Connection connection = DATA_SOURCE.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM test_orders");
             var rs = statement.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to count orders", e);
        }
    }
}
