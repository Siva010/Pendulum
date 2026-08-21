package io.pendulum.core.cron;

import io.pendulum.core.store.JobStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Persistence for cron schedules. */
public final class PostgresCronScheduleStore implements CronScheduleStore {

    private static final String COLUMNS = """
            id, tenant_id, name, cron_expression, timezone, job_type, queue, payload::text AS payload,
            priority, max_attempts, enabled, misfire_policy, catch_up_limit, next_fire_at,
            last_fired_at, fire_count
            """;

    private static final String UPSERT_SQL = """
            INSERT INTO cron_schedules (id, tenant_id, name, cron_expression, timezone, job_type,
                                        queue, payload, priority, max_attempts, enabled,
                                        misfire_policy, catch_up_limit, next_fire_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?, ?, CAST(? AS timestamptz))
            ON CONFLICT (tenant_id, name) DO UPDATE
               SET cron_expression = EXCLUDED.cron_expression,
                   timezone        = EXCLUDED.timezone,
                   job_type        = EXCLUDED.job_type,
                   queue           = EXCLUDED.queue,
                   payload         = EXCLUDED.payload,
                   priority        = EXCLUDED.priority,
                   max_attempts    = EXCLUDED.max_attempts,
                   enabled         = EXCLUDED.enabled,
                   misfire_policy  = EXCLUDED.misfire_policy,
                   catch_up_limit  = EXCLUDED.catch_up_limit,
                   next_fire_at    = EXCLUDED.next_fire_at,
                   updated_at      = now()
            RETURNING id
            """;

    /**
     * Claim due schedules.
     *
     * <p>The subquery captures {@code next_fire_at} as {@code due_at} <em>before</em> the UPDATE
     * overwrites it, and RETURNING reads it back from there. RETURNING on the target table would
     * hand back the new value, and the old one is precisely what catch-up needs — without it a
     * ticker knows a schedule is overdue but not by how much.
     *
     * <p>Pushing {@code next_fire_at} forward is the claim: another ticker sees nothing due. If this
     * ticker dies before recording the real next time, the lease lapses and the schedule is picked
     * up again — at worst re-firing an occurrence whose idempotency key already exists, which the
     * unique index absorbs.
     */
    private static final String CLAIM_DUE_SQL = """
            UPDATE cron_schedules c
               SET next_fire_at = now() + make_interval(secs => CAST(? AS double precision)),
                   updated_at   = now()
              FROM (SELECT id, next_fire_at AS due_at
                      FROM cron_schedules
                     WHERE enabled
                       AND next_fire_at <= now()
                     ORDER BY next_fire_at
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED) due
             WHERE c.id = due.id
            RETURNING c.id, c.tenant_id, c.name, c.cron_expression, c.timezone, c.job_type, c.queue,
                      c.payload::text AS payload, c.priority, c.max_attempts, c.enabled,
                      c.misfire_policy, c.catch_up_limit, due.due_at AS next_fire_at,
                      c.last_fired_at, c.fire_count
            """;

    private static final String RECORD_FIRED_SQL = """
            UPDATE cron_schedules
               SET next_fire_at  = CAST(? AS timestamptz),
                   last_fired_at = CASE WHEN ? > 0 THEN now() ELSE last_fired_at END,
                   fire_count    = fire_count + ?,
                   last_error    = NULL,
                   updated_at    = now()
             WHERE id = ?
            """;

    private static final String DISABLE_SQL = """
            UPDATE cron_schedules
               SET enabled = false, last_error = ?, updated_at = now()
             WHERE id = ?
            """;

    private final DataSource dataSource;

    public PostgresCronScheduleStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public UUID save(CronSchedule schedule, Instant firstFireAt) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPSERT_SQL)) {

            int index = 1;
            statement.setObject(index++, schedule.id());
            statement.setString(index++, schedule.tenantId());
            statement.setString(index++, schedule.name());
            statement.setString(index++, schedule.cronExpression());
            statement.setString(index++, schedule.timezone());
            statement.setString(index++, schedule.jobType());
            statement.setString(index++, schedule.queue());
            statement.setString(index++, schedule.payload());
            statement.setInt(index++, schedule.priority());
            statement.setInt(index++, schedule.maxAttempts());
            statement.setBoolean(index++, schedule.enabled());
            statement.setString(index++, schedule.misfirePolicy().name());
            statement.setInt(index++, schedule.catchUpLimit());
            statement.setObject(index, OffsetDateTime.ofInstant(firstFireAt, ZoneOffset.UTC));

            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getObject("id", UUID.class);
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to save cron schedule " + schedule.name(), e);
        }
    }

    @Override
    public List<CronSchedule> claimDue(int limit, Duration claimFor) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(CLAIM_DUE_SQL)) {

            statement.setDouble(1, claimFor.toNanos() / 1_000_000_000.0d);
            statement.setInt(2, limit);

            List<CronSchedule> claimed = new ArrayList<>(limit);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    claimed.add(map(rs));
                }
            }
            return claimed;
        } catch (SQLException e) {
            throw new JobStoreException("failed to claim due cron schedules", e);
        }
    }

    @Override
    public void recordFired(UUID id, Instant nextFireAt, int firedCount) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(RECORD_FIRED_SQL)) {
            statement.setObject(1, OffsetDateTime.ofInstant(nextFireAt, ZoneOffset.UTC));
            statement.setInt(2, firedCount);
            statement.setInt(3, firedCount);
            statement.setObject(4, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("failed to record cron fire for " + id, e);
        }
    }

    @Override
    public void disable(UUID id, String reason) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(DISABLE_SQL)) {
            statement.setString(1, reason);
            statement.setObject(2, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("failed to disable cron schedule " + id, e);
        }
    }

    @Override
    public Optional<CronSchedule> find(UUID id) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT " + COLUMNS + " FROM cron_schedules WHERE id = ?")) {
            statement.setObject(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to load cron schedule " + id, e);
        }
    }

    @Override
    public List<CronSchedule> findAll(String tenantId) {
        String sql = "SELECT " + COLUMNS + " FROM cron_schedules"
                + (tenantId == null ? "" : " WHERE tenant_id = ?") + " ORDER BY name";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            if (tenantId != null) {
                statement.setString(1, tenantId);
            }
            List<CronSchedule> schedules = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    schedules.add(map(rs));
                }
            }
            return schedules;
        } catch (SQLException e) {
            throw new JobStoreException("failed to list cron schedules", e);
        }
    }

    private static CronSchedule map(ResultSet rs) throws SQLException {
        return new CronSchedule(
                rs.getObject("id", UUID.class),
                rs.getString("tenant_id"),
                rs.getString("name"),
                rs.getString("cron_expression"),
                rs.getString("timezone"),
                rs.getString("job_type"),
                rs.getString("queue"),
                rs.getString("payload"),
                rs.getInt("priority"),
                rs.getInt("max_attempts"),
                rs.getBoolean("enabled"),
                MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                rs.getInt("catch_up_limit"),
                instant(rs, "next_fire_at"),
                instant(rs, "last_fired_at"),
                rs.getLong("fire_count"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
