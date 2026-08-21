package io.pendulum.core.workflow;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps {@code workflow_type} to its definition. */
public final class WorkflowRegistry {

    private final Map<String, Workflow> workflows = new ConcurrentHashMap<>();

    public WorkflowRegistry register(Workflow workflow) {
        Workflow previous = workflows.putIfAbsent(workflow.type(), workflow);
        if (previous != null) {
            throw new IllegalStateException("a workflow is already registered as '" + workflow.type() + "'");
        }
        return this;
    }

    public Optional<Workflow> lookup(String type) {
        return Optional.ofNullable(workflows.get(type));
    }
}
