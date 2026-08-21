package io.pendulum.core.workflow;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.JobContext;
import io.pendulum.core.engine.JobHandler;
import io.pendulum.core.engine.LeaseLostException;
import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.retry.TerminalJobException;
import io.pendulum.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Runs workflows as jobs.
 *
 * <h2>The model</h2>
 * A run is driven by exactly one job, whose handler walks the steps in order. Each step's output is
 * committed before the next begins, and {@code completed_steps} is advanced only after that commit.
 * When the job is retried — because the handler threw, or because a worker died and the reaper
 * requeued it — execution resumes at {@code completed_steps} and replays the earlier outputs from
 * the database instead of re-running them.
 *
 * <p>That ordering is the whole guarantee, and it is deliberately conservative: a crash *between*
 * a step's side effect and its commit re-runs that step. The alternative ordering — advance first,
 * commit after — would instead skip a step that never ran, silently. Re-running a step is a problem
 * an idempotent step handles; skipping one is data loss nobody notices.
 *
 * <h2>What this costs</h2>
 * One worker is occupied for the whole run. That is fine for workflows measured in seconds and
 * wrong for ones that wait on a human for three days; those want a continuation model, where a step
 * that must wait re-enqueues the run with a delay and releases the worker. The tables here already
 * support it — {@code completed_steps} is all a continuation needs — but the executor does not do
 * it yet, and pretending otherwise would be the wrong claim to make about a scheduler.
 */
public final class WorkflowEngine {

    /** The reserved job type that drives every workflow run. */
    public static final String JOB_TYPE = "pendulum.workflow";

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowStore runs;
    private final JobStore jobs;
    private final WorkflowRegistry registry;
    private final DataSource dataSource;

    public WorkflowEngine(WorkflowStore runs, JobStore jobs, WorkflowRegistry registry, DataSource dataSource) {
        this.runs = runs;
        this.jobs = jobs;
        this.registry = registry;
        this.dataSource = dataSource;
    }

    /** Register the driver handler so workers can execute runs. */
    public void registerWith(HandlerRegistry handlers) {
        handlers.register(JOB_TYPE, handler());
    }

    /** Start a run in its own transaction. */
    public UUID start(String tenantId, String workflowType, String input) {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                UUID runId = start(connection, tenantId, workflowType, input);
                connection.commit();
                return runId;
            } catch (RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to start workflow " + workflowType, e);
        }
    }

    /**
     * Start a run inside a transaction the caller owns.
     *
     * <p>The run row and its driving job are inserted together, so a rolled-back business
     * transaction leaves neither — the same guarantee transactional enqueue gives a single job,
     * extended to a whole workflow.
     */
    public UUID start(Connection connection, String tenantId, String workflowType, String input) {
        if (registry.lookup(workflowType).isEmpty()) {
            throw new IllegalArgumentException("no workflow registered as '" + workflowType + "'");
        }
        UUID runId = UUID.randomUUID();
        runs.createRun(connection, runId, tenantId, workflowType, input);

        // The job id is not linked back here on purpose: the run row is not committed yet, so an
        // UPDATE on another connection would see nothing. The executor attaches it on first run.
        jobs.enqueue(connection, NewJob.of(tenantId, JOB_TYPE)
                .payload(JsonPayloads.toJson(Map.of("runId", runId.toString())))
                // Deterministic, so a retried start cannot produce two jobs for one run.
                .idempotencyKey("workflow:" + runId)
                .build()).id();

        return runId;
    }

    /** Attach the job id after the transaction commits, for the admin view. */
    public void linkJob(UUID runId, UUID jobId) {
        runs.attachJob(runId, jobId);
    }

    // ------------------------------------------------------------- execution

    private JobHandler handler() {
        return this::execute;
    }

    private void execute(JobContext context) throws Exception {
        UUID runId = UUID.fromString(
                JsonPayloads.fromJson(context.payload(), Map.class).get("runId").toString());

        WorkflowRun run = runs.find(runId).orElseThrow(() ->
                // The run row is gone but its job is not. Retrying cannot conjure it back.
                new TerminalJobException("workflow run " + runId + " no longer exists"));

        if (run.isTerminal()) {
            log.debug("workflow run {} is already {}", runId, run.state());
            return;
        }

        Workflow workflow = registry.lookup(run.workflowType()).orElseThrow(() ->
                new IllegalStateException("no workflow registered as '" + run.workflowType()
                        + "' on this worker"));

        runs.attachJob(runId, context.job().id());

        // Replay: everything already committed, in step order.
        Map<Integer, WorkflowStore.StepRecord> committed = runs.stepResults(runId);
        Map<String, String> outputs = new LinkedHashMap<>();
        for (WorkflowStore.StepRecord record : committed.values()) {
            outputs.put(record.name(), record.output());
        }

        int resumeAt = run.completedSteps();
        if (resumeAt > 0) {
            log.info("resuming workflow {} '{}' at step {}/{} — {} step(s) replayed, not re-run",
                    runId, workflow.type(), resumeAt + 1, workflow.size(), resumeAt);
        }

        // Seed from the replay so a run that resumes with every step already committed still
        // completes with the right result rather than a null.
        String lastOutput = committed.isEmpty() ? null
                : committed.get(committed.keySet().stream().max(Integer::compareTo).orElseThrow()).output();

        for (int index = resumeAt; index < workflow.size(); index++) {
            Workflow.NamedStep step = workflow.stepAt(index);

            // Between steps is the natural cancellation point: nothing is half-done here, so a
            // worker that has lost its lease can stop without leaving the run inconsistent.
            if (!context.leaseHeld()) {
                throw new LeaseLostException(context.job().id(), context.leaseToken());
            }

            WorkflowContext stepContext = new WorkflowContext(
                    runId, run.tenantId(), run.workflowType(), run.input(),
                    index, step.name(), outputs, context::leaseHeld);

            log.debug("workflow {} step {}/{} '{}'", runId, index + 1, workflow.size(), step.name());
            String produced = step.step().execute(stepContext);

            // Commit first, advance second. A crash in between re-runs this step; the reverse
            // ordering would skip a step that never ran.
            String authoritative = runs.recordStep(runId, index, step.name(), produced, context.attempt());
            if (produced != null && !produced.equals(authoritative)) {
                log.warn("workflow {} step '{}' was already committed by another execution; "
                        + "adopting the recorded output", runId, step.name());
            }
            outputs.put(step.name(), authoritative);
            lastOutput = authoritative;
            runs.advanceTo(runId, index + 1);
        }

        runs.complete(runId, lastOutput);
        log.info("workflow {} '{}' completed in {} step(s)", runId, workflow.type(), workflow.size());
    }

    /**
     * Mark a run failed once its driving job is finally dead-lettered.
     *
     * <p>Called by the operator surface rather than the executor: while the job still has attempts
     * left, the run is not failed, it is between tries.
     */
    public void markFailed(UUID runId, String error) {
        runs.fail(runId, error);
    }
}
