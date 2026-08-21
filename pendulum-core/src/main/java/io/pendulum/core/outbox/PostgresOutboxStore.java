package io.pendulum.core.outbox;

import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.store.JobStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class PostgresOutboxStore implements OutboxStore {

    private static final String RECORD_SQL = """
            INSERT INTO outbox (id, tenant_id, destination, payload, headers, message_key, max_attempts)
            VALUES (?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    /**
     * The same {@code SKIP LOCKED} claim as job dispatch, with {@code next_attempt_at} serving as
     * the visibility timeout rather than a separate lease column.
     */
    private static final String CLAIM_SQL = """
            UPDATE outbox o
               SET next_attempt_at = now() + make_interval(secs => CAST(? AS double precision)),
                   attempts        = o.attempts + 1
              FROM (SELECT id
                      FROM outbox
                     WHERE state = 'PENDING'
                       AND next_attempt_at <= now()
                     ORDER BY next_attempt_at, id
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED) candidate
             WHERE o.id = candidate.id
            RETURNING o.id, o.tenant_id, o.destination, o.payload::text AS payload,
                      o.headers::text AS headers, o.message_key, o.attempts, o.max_attempts
            """;

    private static final String MARK_PUBLISHED_SQL = """
            UPDATE outbox
               SET state = 'PUBLISHED', published_at = now(), last_error = NULL
             WHERE id = ? AND state = 'PENDING'
            """;

    private static final String MARK_FAILED_SQL = """
            UPDATE outbox
               SET next_attempt_at = now() + make_interval(secs => CAST(? AS double precision)),
                   last_error      = ?
             WHERE id = ? AND state = 'PENDING'
            """;

    private static final String MARK_DEAD_LETTERED_SQL = """
            UPDATE outbox
               SET state = 'DEAD_LETTERED', last_error = ?, published_at = now()
             WHERE id = ? AND state = 'PENDING'
            """;

    private final DataSource dataSource;

    public PostgresOutboxStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void record(Connection connection, OutboxMessage message) {
        // Not closed, not committed: this statement belongs to the caller's transaction, which is
        // the only reason the outbox pattern works at all.
        try (PreparedStatement statement = connection.prepareStatement(RECORD_SQL)) {
            statement.setObject(1, message.id());
            statement.setString(2, message.tenantId());
            statement.setString(3, message.destination());
            statement.setString(4, message.payload());
            statement.setString(5, JsonPayloads.toJson(message.headers()));
            if (message.messageKey() == null) {
                statement.setNull(6, Types.VARCHAR);
            } else {
                statement.setString(6, message.messageKey());
            }
            statement.setInt(7, message.maxAttempts());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("failed to record outbox message " + message.id(), e);
        }
    }

    @Override
    public List<OutboxMessage> claimForPublishing(int limit, Duration visibilityTimeout) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(CLAIM_SQL)) {

            statement.setDouble(1, visibilityTimeout.toNanos() / 1_000_000_000.0d);
            statement.setInt(2, limit);

            List<OutboxMessage> claimed = new ArrayList<>(limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    claimed.add(new OutboxMessage(
                            rs.getObject("id", UUID.class),
                            rs.getString("tenant_id"),
                            rs.getString("destination"),
                            rs.getString("payload"),
                            readHeaders(rs.getString("headers")),
                            rs.getString("message_key"),
                            rs.getInt("attempts"),
                            rs.getInt("max_attempts")));
                }
            }
            return claimed;
        } catch (SQLException e) {
            throw new JobStoreException("failed to claim outbox messages", e);
        }
    }

    @Override
    public boolean markPublished(UUID id) {
        return update(MARK_PUBLISHED_SQL, "markPublished", statement -> statement.setObject(1, id));
    }

    @Override
    public boolean markFailed(UUID id, Duration delay, String error) {
        return update(MARK_FAILED_SQL, "markFailed", statement -> {
            statement.setDouble(1, delay.toNanos() / 1_000_000_000.0d);
            statement.setString(2, truncate(error));
            statement.setObject(3, id);
        });
    }

    @Override
    public boolean markDeadLettered(UUID id, String error) {
        return update(MARK_DEAD_LETTERED_SQL, "markDeadLettered", statement -> {
            statement.setString(1, truncate(error));
            statement.setObject(2, id);
        });
    }

    @Override
    public long countInState(String state) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM outbox WHERE state = ?")) {
            statement.setString(1, state);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to count outbox messages in state " + state, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> readHeaders(String json) {
        if (json == null || json.isBlank()) return Map.of();
        return JsonPayloads.fromJson(json, Map.class);
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private boolean update(String sql, String operation, Binder binder) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new JobStoreException("failed to " + operation, e);
        }
    }

    private static String truncate(String error) {
        if (error == null) return null;
        return error.length() <= 2000 ? error : error.substring(0, 2000) + "... [truncated]";
    }
}
