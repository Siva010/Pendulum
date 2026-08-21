package io.pendulum.core.workflow;

/**
 * One unit of durable work.
 *
 * <p>A step's returned output is committed before the next step begins, which is what lets a crash
 * resume rather than restart. That also constrains what a step may be: it must be safe to run
 * again, because a worker that completes a step's side effect and dies before committing the output
 * will run it a second time. Same at-least-once contract as a job handler, at finer granularity.
 */
@FunctionalInterface
public interface WorkflowStep {

    /**
     * @return this step's output as JSON, or {@code null}. It is persisted and replayed verbatim on
     *         resume, so it must contain everything a later step needs — a step that stashes state
     *         in a field instead of returning it will find that field empty after a crash.
     */
    String execute(WorkflowContext context) throws Exception;
}
