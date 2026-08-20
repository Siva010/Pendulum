package io.pendulum.core.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * An enqueue request. Separate from {@link Job} because the fields a caller supplies and
 * the fields the engine owns (state, attempt, lease) are different sets, and collapsing
 * them into one type is how you end up with a constructor full of nulls.
 */
public record NewJob(
        UUID id,
        String tenantId,
        String queue,
        String jobType,
        String payload,
        int priority,
        Schedule schedule,
        int maxAttempts,
        String idempotencyKey
) {

    public static final String DEFAULT_QUEUE = "default";
    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    public NewJob {
        if (id == null) throw new IllegalArgumentException("id is required");
        if (tenantId == null || tenantId.isBlank()) throw new IllegalArgumentException("tenantId is required");
        if (queue == null || queue.isBlank()) throw new IllegalArgumentException("queue is required");
        if (jobType == null || jobType.isBlank()) throw new IllegalArgumentException("jobType is required");
        if (schedule == null) throw new IllegalArgumentException("schedule is required");
        if (maxAttempts <= 0) throw new IllegalArgumentException("maxAttempts must be positive");
        if (payload == null) payload = "{}";
    }

    public static Builder of(String tenantId, String jobType) {
        return new Builder(tenantId, jobType);
    }

    /**
     * A builder, because this is one of the few places where it relieves a real force:
     * nine fields, seven with sensible defaults, at a boundary called by application
     * code that should not have to pass nulls positionally.
     */
    public static final class Builder {
        private final String tenantId;
        private final String jobType;
        private UUID id = UUID.randomUUID();
        private String queue = DEFAULT_QUEUE;
        private String payload = "{}";
        private int priority = 0;
        private Schedule schedule = Schedule.now();
        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        private String idempotencyKey;

        private Builder(String tenantId, String jobType) {
            this.tenantId = tenantId;
            this.jobType = jobType;
        }

        public Builder id(UUID id) { this.id = id; return this; }
        public Builder queue(String queue) { this.queue = queue; return this; }
        public Builder payload(String json) { this.payload = json; return this; }
        public Builder priority(int priority) { this.priority = priority; return this; }
        public Builder maxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; return this; }
        public Builder idempotencyKey(String key) { this.idempotencyKey = key; return this; }
        public Builder schedule(Schedule schedule) { this.schedule = schedule; return this; }
        public Builder runAfter(Duration delay) { return schedule(Schedule.after(delay)); }
        public Builder runAt(Instant when) { return schedule(Schedule.at(when)); }

        public NewJob build() {
            return new NewJob(id, tenantId, queue, jobType, payload, priority, schedule, maxAttempts, idempotencyKey);
        }
    }
}
