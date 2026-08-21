package io.pendulum.core.workflow;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * What a step is given: the run's input, the outputs of every step already completed, and a way to
 * check the lease.
 *
 * <p>Outputs come from {@code step_results}, not from memory, so a resumed run sees exactly what
 * the original execution produced. That is what makes resume meaningful: step four gets step two's
 * committed answer, not a recomputed one that might differ.
 */
public record WorkflowContext(
        UUID runId,
        String tenantId,
        String workflowType,
        String input,
        int stepIndex,
        String stepName,
        Map<String, String> completedOutputs,
        BooleanSupplier leaseHolder
) {

    public WorkflowContext {
        completedOutputs = Map.copyOf(completedOutputs);
    }

    /** The committed output of an earlier step, by name. */
    public Optional<String> outputOf(String stepName) {
        return Optional.ofNullable(completedOutputs.get(stepName));
    }

    /** The committed output of an earlier step, or a failure naming what is missing. */
    public String requireOutputOf(String stepName) {
        String output = completedOutputs.get(stepName);
        if (output == null) {
            throw new IllegalStateException(
                    "step '" + this.stepName + "' needs the output of '" + stepName
                    + "', which has not completed (available: " + completedOutputs.keySet() + ")");
        }
        return output;
    }

    /** True while this worker still owns the run's job. */
    public boolean leaseHeld() {
        return leaseHolder.getAsBoolean();
    }
}
