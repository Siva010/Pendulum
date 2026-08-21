package io.pendulum.core.workflow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An ordered list of named steps.
 *
 * <pre>{@code
 * Workflow onboarding = Workflow.named("onboard-customer")
 *         .step("create-account", ctx -> accounts.create(ctx.input()))
 *         .step("charge-setup-fee", ctx -> billing.charge(ctx.outputOf("create-account")))
 *         .step("send-welcome", ctx -> mailer.welcome(ctx.outputOf("create-account")))
 *         .build();
 * }</pre>
 *
 * <p>Steps are addressed by index at runtime and by name in code. The index is what
 * {@code completed_steps} refers to, so <strong>reordering or removing a step changes the meaning
 * of every in-flight run</strong>: a run that had completed three steps will resume at whatever now
 * occupies position four. Versioning workflows properly means registering a new type rather than
 * editing an existing one, and this is the same constraint every durable execution engine has.
 */
public record Workflow(String type, List<NamedStep> steps) {

    public record NamedStep(String name, WorkflowStep step) {}

    public Workflow {
        if (type == null || type.isBlank()) throw new IllegalArgumentException("workflow type is required");
        steps = List.copyOf(steps);
        if (steps.isEmpty()) throw new IllegalArgumentException("a workflow needs at least one step");

        Map<String, Integer> seen = new LinkedHashMap<>();
        for (int i = 0; i < steps.size(); i++) {
            Integer previous = seen.put(steps.get(i).name(), i);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "duplicate step name '" + steps.get(i).name() + "' at positions "
                        + previous + " and " + i + "; outputs are looked up by name");
            }
        }
    }

    public static Builder named(String type) {
        return new Builder(type);
    }

    public int size() {
        return steps.size();
    }

    public NamedStep stepAt(int index) {
        return steps.get(index);
    }

    public static final class Builder {
        private final String type;
        private final List<NamedStep> steps = new ArrayList<>();

        private Builder(String type) {
            this.type = type;
        }

        public Builder step(String name, WorkflowStep step) {
            steps.add(new NamedStep(name, step));
            return this;
        }

        public Workflow build() {
            return new Workflow(type, steps);
        }
    }
}
