package io.pendulum.core.workflow;

import io.pendulum.core.store.JobStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public final class PostgresWorkflowStore implements WorkflowStore {

    private static final String RUN_COLUMNS = """
            id, tenant_id, workflow_type, input::text AS input, state, completed_steps, job_id,
            result::text AS result, last_error, created_at, updated_at, completed_at
            """;

    private static final String CREATE_RUN_SQL = """
            INSERT INTO workflow_runs (id, tenant_id, workflow_type, input, state)
            VALUES (?, ?, ?, CAST(? AS jsonb), 'PENDING')
            """;

    /**
     * Commit a step's output.
     *
     * <p>{@code ON CONFLICT DO NOTHING} rather than an upsert, and the difference matters. If two
     * workers both executed this step — one stalled past its lease, the other took over — the first
     * committed output is the run's authoritative history and must not be overwritten. The loser
     * reads the winner's value back and continues from it, so both executions converge on identical
     * state instead of the run's history depending on who finished last.
     */
    private static final String RECORD_STEP_SQL = """
            INSERT INTO step_results (run_id, step_index, step_name, output, attempts)
            VALUES (?, ?, ?, CAST(? AS jsonb), ?)
            ON CONFLICT (run_id, step_index) DO NOTHING
            RETURNING output::text AS output
            """;

    /**
     * {@code GREATEST} because progress must never move backwards. A slow worker finishing step 2
     * after another worker has already reached step 4 would otherwise rewind the run and cause
     * steps 3 and 4 to execute a second time.
     */
    private static final String ADVANCE_SQL = """
            UPDATE workflow_runs
               SET completed_steps = GREATEST(completed_steps, ?),
                   state           = 'RUNNING',
                   updated_at      = now()
             WHERE id = ?
            """;

    private static final String COMPLETE_SQL = """
            UPDATE workflow_runs
               SET state        = 'SUCCEEDED',
                   result       = CAST(? AS jsonb),
                   completed_at = now(),
                   last_error   = NULL,
                   updated_at   = now()
             WHERE id = ? AND state <> 'SUCCEEDED'
            """;

    private static final String FAIL_SQL = """
            UPDATE workflow_runs
               SET state        = 'FAILED',
                   last_error   = ?,
                   completed_at = now(),
                   updated_at   = now()
             WHERE id = ?
            """;

    private final DataSource dataSource;

    public PostgresWorkflowStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createRun(Connection connection, UUID id, String tenantId, String workflowType, String input) {
        // On the caller's connection, so starting a workflow can join a business transaction the
        // same way enqueueing a job can.
        try (PreparedStatement statement = connection.prepareStatement(CREATE_RUN_SQL)) {
            statement.setObject(1, id);
            statement.setString(2, tenantId);
            statement.setString(3, workflowType);
            statement.setString(4, input == null ? "{}" : input);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("failed to create workflow run " + id, e);
        }
    }

    @Override
    public void attachJob(UUID runId, UUID jobId) {
        execute("UPDATE workflow_runs SET job_id = ?, updated_at = now() WHERE id = ?",
                statement -> {
                    statement.setObject(1, jobId);
                    statement.setObject(2, runId);
                });
    }

    @Override
    public Optional<WorkflowRun> find(UUID id) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT " + RUN_COLUMNS + " FROM workflow_runs WHERE id = ?")) {
            statement.setObject(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(mapRun(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to load workflow run " + id, e);
        }
    }

    /** Ordered by step index so replay reconstructs the run's history in execution order. */
    @Override
    public Map<Integer, StepRecord> stepResults(UUID runId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT step_index, step_name, output::text AS output, attempts
                       FROM step_results
                      WHERE run_id = ?
                      ORDER BY step_index
                     """)) {
            statement.setObject(1, runId);
            Map<Integer, StepRecord> results = new LinkedHashMap<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    results.put(rs.getInt("step_index"), new StepRecord(
                            rs.getInt("step_index"),
                            rs.getString("step_name"),
                            rs.getString("output"),
                            rs.getInt("attempts")));
                }
            }
            return results;
        } catch (SQLException e) {
            throw new JobStoreException("failed to load step results for run " + runId, e);
        }
    }

    @Override
    public String recordStep(UUID runId, int stepIndex, String stepName, String output, int attempts) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(RECORD_STEP_SQL)) {
                statement.setObject(1, runId);
                statement.setInt(2, stepIndex);
                statement.setString(3, stepName);
                if (output == null) {
                    statement.setNull(4, Types.OTHER);
                } else {
                    statement.setString(4, output);
                }
                statement.setInt(5, attempts);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        // The stored form, not the string we passed in. jsonb normalises whitespace
                        // and key order, so returning the raw input here would hand a step's
                        // successors different bytes on a fresh run than on a resumed one — and
                        // "identical on replay" is the property the whole feature rests on.
                        return rs.getString("output");
                    }
                }
            }
            // Someone else committed this step first. Their value is the run's history; adopt it so
            // both executions proceed from identical state.
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT output::text AS output FROM step_results WHERE run_id = ? AND step_index = ?")) {
                statement.setObject(1, runId);
                statement.setInt(2, stepIndex);
                try (ResultSet rs = statement.executeQuery()) {
                    return rs.next() ? rs.getString("output") : output;
                }
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to record step " + stepIndex + " of run " + runId, e);
        }
    }

    @Override
    public void advanceTo(UUID runId, int completedSteps) {
        execute(ADVANCE_SQL, statement -> {
            statement.setInt(1, completedSteps);
            statement.setObject(2, runId);
        });
    }

    @Override
    public void complete(UUID runId, String result) {
        execute(COMPLETE_SQL, statement -> {
            if (result == null) {
                statement.setNull(1, Types.OTHER);
            } else {
                statement.setString(1, result);
            }
            statement.setObject(2, runId);
        });
    }

    @Override
    public void fail(UUID runId, String error) {
        execute(FAIL_SQL, statement -> {
            statement.setString(1, error == null || error.length() <= 2000
                    ? error : error.substring(0, 2000) + "... [truncated]");
            statement.setObject(2, runId);
        });
    }

    @Override
    public long countInState(String state) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM workflow_runs WHERE state = ?")) {
            statement.setString(1, state);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new JobStoreException("failed to count workflow runs", e);
        }
    }

    @Override
    public java.util.List<WorkflowRun> recentRuns(String tenantId, int limit) {
        String sql = "SELECT " + RUN_COLUMNS + " FROM workflow_runs"
                + (tenantId == null ? "" : " WHERE tenant_id = ?")
                + " ORDER BY created_at DESC LIMIT ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (tenantId != null) {
                statement.setString(index++, tenantId);
            }
            statement.setInt(index, Math.clamp(limit, 1, 500));

            java.util.List<WorkflowRun> runs = new java.util.ArrayList<>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    runs.add(mapRun(rs));
                }
            }
            return runs;
        } catch (SQLException e) {
            throw new JobStoreException("failed to list workflow runs", e);
        }
    }

    private static WorkflowRun mapRun(ResultSet rs) throws SQLException {
        return new WorkflowRun(
                rs.getObject("id", UUID.class),
                rs.getString("tenant_id"),
                rs.getString("workflow_type"),
                rs.getString("input"),
                rs.getString("state"),
                rs.getInt("completed_steps"),
                rs.getObject("job_id", UUID.class),
                rs.getString("result"),
                rs.getString("last_error"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "completed_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private void execute(String sql, Binder binder) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new JobStoreException("workflow store update failed", e);
        }
    }
}
