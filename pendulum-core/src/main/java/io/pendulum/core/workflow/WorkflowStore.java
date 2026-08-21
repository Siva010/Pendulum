package io.pendulum.core.workflow;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Persistence for workflow runs and their committed step outputs. */
public interface WorkflowStore {

    /** One committed step. */
    record StepRecord(int index, String name, String output, int attempts) {}

    /** Create a run on the caller's connection, so starting one can join a business transaction. */
    void createRun(Connection connection, UUID id, String tenantId, String workflowType, String input);

    void attachJob(UUID runId, UUID jobId);

    Optional<WorkflowRun> find(UUID id);

    /** Every committed step, keyed by index. */
    Map<Integer, StepRecord> stepResults(UUID runId);

    /**
     * Commit a step's output, first writer winning.
     *
     * @return the authoritative output — the one just written, or the one already present if
     *         another execution committed this step first
     */
    String recordStep(UUID runId, int stepIndex, String stepName, String output, int attempts);

    /** Move the resume point forward. Never backwards, even if called out of order. */
    void advanceTo(UUID runId, int completedSteps);

    void complete(UUID runId, String result);

    void fail(UUID runId, String error);

    long countInState(String state);

    /** Most recent runs, newest first, for the admin surface. */
    java.util.List<WorkflowRun> recentRuns(String tenantId, int limit);
}
