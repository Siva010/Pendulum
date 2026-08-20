package io.pendulum.core.store;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.JobState;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.domain.Schedule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The Postgres implementation. Plain JDBC on purpose: there is no ORM that expresses
 * {@code UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) ... RETURNING *} without a
 * native-query escape hatch, and the moment you reach for that escape hatch the ORM has
 * stopped earning its keep.
 *
 * <p>Every statement here is single-round-trip and auto-committing. The engine deliberately
 * owns no long transactions: a transaction held open across a handler invocation would pin a
 * connection and an MVCC snapshot for the whole duration of a job, which is how a queue table
 * accumulates bloat and a connection pool runs dry.
 */
public final class PostgresJobStore implements JobStore {

    private static final String JOB_COLUMNS = """
            id, tenant_id, queue, job_type, payload::text AS payload, state, priority, run_at,
            attempt, max_attempts, lease_token, lease_owner, lease_expires_at,
            idempotency_key, last_error, created_at, updated_at
            """;

    private static final String RETURNING_JOB = """
            RETURNING j.id, j.tenant_id, j.queue, j.job_type, j.payload::text AS payload, j.state,
                      j.priority, j.run_at, j.attempt, j.max_attempts, j.lease_token, j.lease_owner,
                      j.lease_expires_at, j.idempotency_key, j.last_error, j.created_at, j.updated_at
            """;

    /**
     * The dispatch statement — the single most important query in the project.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} in the subquery is what makes N pollers contention-free: a
     * row another worker is already claiming is stepped over rather than waited on, so claim
     * latency stays flat as workers are added instead of degrading into a lock convoy.
     *
     * <p>{@code nextval()} in the SET clause issues the fencing token inside the same statement that
     * grants the lease, so a token can never be observed without the lease that goes with it.
     *
     * <p>{@code attempt + 1} happens here, at claim, not at failure: a handler that SIGKILLs the JVM
     * never reaches a failure path, and a counter that only advanced on clean failures would let
     * such a poison pill cycle forever.
     */
    private static final String CLAIM_SQL = """
            UPDATE jobs j
               SET state             = 'LEASED',
                   lease_token       = nextval('pendulum_lease_token_seq'),
                   lease_owner       = ?,
                   lease_expires_at  = now() + make_interval(secs => CAST(? AS double precision)),
                   last_heartbeat_at = now(),
                   attempt           = j.attempt + 1,
                   updated_at        = now()
              FROM (SELECT id
                      FROM jobs
                     WHERE state = 'PENDING'
                       AND queue = ?
                       AND run_at <= now()
                     ORDER BY priority DESC, run_at, id
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED) candidate
             WHERE j.id = candidate.id
            """ + RETURNING_JOB;

    /**
     * The reaper. Same SKIP LOCKED discipline so concurrent reapers cannot collide, and the same
     * attempt-budget check so an orphan that has already burned its attempts is dead-lettered
     * rather than requeued forever.
     */
    private static final String REAP_SQL = """
            UPDATE jobs j
               SET state        = CASE WHEN j.attempt < j.max_attempts THEN 'PENDING' ELSE 'DEAD_LETTERED' END,
                   run_at       = now(),
                   completed_at = CASE WHEN j.attempt < j.max_attempts THEN NULL ELSE now() END,
                   last_error   = 'lease expired (owner=' || COALESCE(j.lease_owner, '?')
                                  || ', attempt=' || j.attempt || ')',
                   lease_owner      = NULL,
                   lease_expires_at = NULL,
                   updated_at       = now()
              FROM (SELECT id
                      FROM jobs
                     WHERE state IN ('LEASED', 'RUNNING')
                       AND lease_expires_at < now()
                     ORDER BY lease_expires_at
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED) expired
             WHERE j.id = expired.id
            """;

    /** The fencing predicate, appended to every write that reports an execution outcome. */
    private static final String FENCE =
            " WHERE id = ? AND lease_token = ? AND state IN ('LEASED', 'RUNNING')";

    private static final String MARK_RUNNING_SQL =
            "UPDATE jobs SET state = 'RUNNING', updated_at = now()"
            + " WHERE id = ? AND lease_token = ? AND state = 'LEASED'";

    private static final String HEARTBEAT_SQL = """
            UPDATE jobs
               SET lease_expires_at  = now() + make_interval(secs => CAST(? AS double precision)),
                   last_heartbeat_at = now(),
                   updated_at        = now()
            """ + FENCE;

    private static final String COMPLETE_SQL = """
            UPDATE jobs
               SET state            = 'SUCCEEDED',
                   completed_at     = now(),
                   lease_expires_at = NULL,
                   lease_owner      = NULL,
                   updated_at       = now()
            """ + FENCE;

    private static final String RETRY_LATER_SQL = """
            UPDATE jobs
               SET state            = 'PENDING',
                   run_at           = now() + make_interval(secs => CAST(? AS double precision)),
                   last_error       = ?,
                   lease_expires_at = NULL,
                   lease_owner      = NULL,
                   updated_at       = now()
            """ + FENCE;

    private static final String DEAD_LETTER_SQL = """
            UPDATE jobs
               SET state            = 'DEAD_LETTERED',
                   last_error       = ?,
                   completed_at     = now(),
                   lease_expires_at = NULL,
                   lease_owner      = NULL,
                   updated_at       = now()
            """ + FENCE;

    /** Note the state predicate: LEASED only. A RUNNING job may already have had effects. */
    private static final String RELEASE_SQL = """
            UPDATE jobs
               SET state            = 'PENDING',
                   run_at           = now(),
                   attempt          = GREATEST(0, attempt - 1),
                   lease_expires_at = NULL,
                   lease_owner      = NULL,
                   updated_at       = now()
             WHERE id = ? AND lease_token = ? AND state = 'LEASED'
            """;

    private final DataSource dataSource;

    public PostgresJobStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public EnqueueResult enqueue(NewJob job) {
        String runAtExpression = switch (job.schedule()) {
            case Schedule.Now n   -> "now()";
            case Schedule.After a -> "now() + make_interval(secs => CAST(? AS double precision))";
            case Schedule.At at   -> "CAST(? AS timestamptz)";
        };

        String sql = """
                INSERT INTO jobs (id, tenant_id, queue, job_type, payload, state, priority, run_at,
                                  max_attempts, idempotency_key)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb), 'PENDING', ?, %s, ?, ?)
                ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING id
                """.formatted(runAtExpression);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            int index = 1;
            statement.setObject(index++, job.id());
            statement.setString(index++, job.tenantId());
            statement.setString(index++, job.queue());
            statement.setString(index++, job.jobType());
            statement.setString(index++, job.payload());
            statement.setInt(index++, job.priority());
            switch (job.schedule()) {
                case Schedule.Now n   -> { }
                case Schedule.After a -> statement.setDouble(index++, toSeconds(a.delay()));
                case Schedule.At at   -> statement.setObject(index++, OffsetDateTime.ofInstant(at.when(), ZoneOffset.UTC));
            }
            statement.setInt(index++, job.maxAttempts());
            if (job.idempotencyKey() == null) {
                statement.setNull(index, Types.VARCHAR);
            } else {
                statement.setString(index, job.idempotencyKey());
            }

            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new EnqueueResult(rs.getObject("id", UUID.class), true);
                }
            }
            // DO NOTHING fired: a job already owns this idempotency key. The unique index did the
            // real work — the application never checks-then-inserts, because that race is the bug.
            return new EnqueueResult(
                    findByIdempotencyKey(connection, job.tenantId(), job.idempotencyKey()), false);

        } catch (SQLException e) {
            throw new JobStoreException("failed to enqueue job " + job.id(), e);
        }
    }

    private UUID findByIdempotencyKey(Connection connection, String tenantId, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT id FROM jobs WHERE tenant_id = ? AND idempotency_key = ?")) {
            statement.setString(1, tenantId);
            statement.setString(2, key);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getObject("id", UUID.class) : null;
            }
        }
    }

    @Override
    public List<Job> claim(String queue, String owner, int limit, Duration leaseDuration) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(CLAIM_SQL)) {

            statement.setString(1, owner);
            statement.setDouble(2, toSeconds(leaseDuration));
            statement.setString(3, queue);
            statement.setInt(4, limit);

            List<Job> claimed = new ArrayList<>(limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    claimed.add(mapJob(rs));
                }
            }
            return claimed;

        } catch (SQLException e) {
            throw new JobStoreException("failed to claim jobs on queue " + queue, e);
        }
    }

    @Override
    public boolean markRunning(UUID id, LeaseToken token) {
        return fencedUpdate(MARK_RUNNING_SQL, "markRunning", statement -> {
            statement.setObject(1, id);
            statement.setLong(2, token.value());
        });
    }

    @Override
    public boolean heartbeat(UUID id, LeaseToken token, Duration leaseDuration) {
        return fencedUpdate(HEARTBEAT_SQL, "heartbeat", statement -> {
            statement.setDouble(1, toSeconds(leaseDuration));
            statement.setObject(2, id);
            statement.setLong(3, token.value());
        });
    }

    @Override
    public boolean complete(UUID id, LeaseToken token) {
        return fencedUpdate(COMPLETE_SQL, "complete", statement -> {
            statement.setObject(1, id);
            statement.setLong(2, token.value());
        });
    }

    @Override
    public boolean retryLater(UUID id, LeaseToken token, Duration delay, String error) {
        return fencedUpdate(RETRY_LATER_SQL, "retryLater", statement -> {
            statement.setDouble(1, toSeconds(delay));
            statement.setString(2, truncate(error));
            statement.setObject(3, id);
            statement.setLong(4, token.value());
        });
    }

    @Override
    public boolean deadLetter(UUID id, LeaseToken token, String error) {
        return fencedUpdate(DEAD_LETTER_SQL, "deadLetter", statement -> {
            statement.setString(1, truncate(error));
            statement.setObject(2, id);
            statement.setLong(3, token.value());
        });
    }

    @Override
    public boolean release(UUID id, LeaseToken token) {
        return fencedUpdate(RELEASE_SQL, "release", statement -> {
            statement.setObject(1, id);
            statement.setLong(2, token.value());
        });
    }

    @Override
    public int reapExpiredLeases(int limit) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(REAP_SQL)) {
            statement.setInt(1, limit);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("failed to reap expired leases", e);
        }
    }

    @Override
    public Optional<Job> find(UUID id) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT " + JOB_COLUMNS + " FROM jobs WHERE id = ?")) {
            statement.setObject(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(mapJob(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to load job " + id, e);
        }
    }

    @Override
    public Map<String, Long> queueDepth(String queue) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT state, count(*) AS n FROM jobs WHERE queue = ? GROUP BY state")) {
            statement.setString(1, queue);
            Map<String, Long> depth = new HashMap<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    depth.put(rs.getString("state"), rs.getLong("n"));
                }
            }
            return depth;
        } catch (SQLException e) {
            throw new JobStoreException("failed to read queue depth for " + queue, e);
        }
    }

    @Override
    public Instant databaseNow() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT now()")) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class).toInstant();
        } catch (SQLException e) {
            throw new JobStoreException("failed to read database clock", e);
        }
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private boolean fencedUpdate(String sql, String operation, StatementBinder binder) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new JobStoreException("failed to " + operation, e);
        }
    }

    private static Job mapJob(ResultSet rs) throws SQLException {
        String discriminator = rs.getString("state");
        Long leaseToken = (Long) rs.getObject("lease_token");
        String leaseOwner = rs.getString("lease_owner");
        Instant leaseExpiresAt = instant(rs, "lease_expires_at");
        Instant updatedAt = instant(rs, "updated_at");
        String lastError = rs.getString("last_error");

        JobState state = switch (discriminator) {
            case "PENDING"       -> new JobState.Pending();
            case "LEASED"        -> new JobState.Leased(new LeaseToken(leaseToken), leaseOwner, leaseExpiresAt);
            case "RUNNING"       -> new JobState.Running(new LeaseToken(leaseToken), leaseOwner, leaseExpiresAt);
            case "SUCCEEDED"     -> new JobState.Succeeded(updatedAt);
            case "FAILED"        -> new JobState.Failed(lastError);
            case "DEAD_LETTERED" -> new JobState.DeadLettered(lastError, updatedAt);
            default -> throw new JobStoreException("unknown job state '" + discriminator + "'", null);
        };

        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("tenant_id"),
                rs.getString("queue"),
                rs.getString("job_type"),
                rs.getString("payload"),
                state,
                rs.getInt("priority"),
                instant(rs, "run_at"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                rs.getString("idempotency_key"),
                lastError,
                instant(rs, "created_at"),
                updatedAt);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static double toSeconds(Duration duration) {
        return duration.toNanos() / 1_000_000_000.0d;
    }

    /** {@code last_error} is for a human reading the admin UI, not for stack-trace archaeology. */
    private static String truncate(String error) {
        if (error == null) return null;
        return error.length() <= 2000 ? error : error.substring(0, 2000) + "... [truncated]";
    }
}
