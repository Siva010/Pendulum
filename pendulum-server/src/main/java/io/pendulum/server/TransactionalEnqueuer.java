package io.pendulum.server;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.outbox.OutboxMessage;
import io.pendulum.core.outbox.OutboxStore;
import io.pendulum.core.store.JobStore;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.UUID;

/**
 * Enqueue that joins whatever transaction the caller is already in.
 *
 * <pre>{@code
 * @Transactional
 * public void placeOrder(Order order) {
 *     orderRepository.save(order);
 *     enqueuer.enqueue(NewJob.of(order.tenantId(), "send-confirmation")
 *             .payload(JsonPayloads.toJson(order))
 *             .idempotencyKey("confirm:" + order.id())
 *             .build());
 * }
 * }</pre>
 *
 * <p>The order and the job commit together or not at all. There is no window in which the order
 * exists and the email does not, and no phantom job for an order that rolled back.
 *
 * <p>The mechanism is {@link DataSourceUtils}, not {@code dataSource.getConnection()}. Inside a
 * Spring-managed transaction it returns the connection bound to that transaction; outside one it
 * returns a fresh connection that commits on its own. Calling {@code getConnection()} directly
 * would take a <em>second</em> connection from the pool with its own transaction, which would
 * commit independently — reintroducing exactly the bug this class exists to remove, while looking
 * for all the world like it had fixed it.
 */
@Component
public class TransactionalEnqueuer {

    private final DataSource dataSource;
    private final JobStore jobStore;
    private final OutboxStore outboxStore;

    public TransactionalEnqueuer(DataSource dataSource, JobStore jobStore, OutboxStore outboxStore) {
        this.dataSource = dataSource;
        this.jobStore = jobStore;
        this.outboxStore = outboxStore;
    }

    /** Enqueue a job in the caller's transaction. */
    public UUID enqueue(NewJob job) {
        return onTransactionConnection(connection -> jobStore.enqueue(connection, job).id());
    }

    /**
     * Record an outbox message in the caller's transaction, for an effect that has to leave the
     * database. If the destination is Pendulum itself, use {@link #enqueue} instead — routing a job
     * through the outbox adds a relay hop to achieve what one INSERT already guarantees.
     */
    public UUID record(OutboxMessage message) {
        return onTransactionConnection(connection -> {
            outboxStore.record(connection, message);
            return message.id();
        });
    }

    private <T> T onTransactionConnection(ConnectionCallback<T> callback) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            return callback.doWith(connection);
        } finally {
            // Releases only if this connection is not bound to an active transaction; when it is,
            // the transaction manager keeps ownership and closing here would be a bug.
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    @FunctionalInterface
    private interface ConnectionCallback<T> {
        T doWith(Connection connection);
    }
}
