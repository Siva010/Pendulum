package io.pendulum.core.workflow;

import java.time.Instant;
import java.util.UUID;

/** A workflow run as stored. */
public record WorkflowRun(
        UUID id,
        String tenantId,
        String workflowType,
        String input,
        String state,
        int completedSteps,
        UUID jobId,
        String result,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt
) {

    public boolean isTerminal() {
        return "SUCCEEDED".equals(state) || "FAILED".equals(state);
    }
}
