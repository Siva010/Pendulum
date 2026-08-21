package io.pendulum.core.cron;

import java.time.Instant;
import java.util.UUID;

/**
 * A schedule as it exists in the database.
 *
 * @param dueAt the occurrence this schedule was claimed for — the value of {@code next_fire_at}
 *              <em>before</em> the ticker pushed it forward to claim it. Carrying the old value is
 *              what makes catch-up possible at all: without it the ticker knows a schedule is due
 *              but not since when, and cannot tell one missed run from four hundred.
 */
public record CronSchedule(
        UUID id,
        String tenantId,
        String name,
        String cronExpression,
        String timezone,
        String jobType,
        String queue,
        String payload,
        int priority,
        int maxAttempts,
        boolean enabled,
        MisfirePolicy misfirePolicy,
        int catchUpLimit,
        Instant dueAt,
        Instant lastFiredAt,
        long fireCount
) {

    public static Builder of(String tenantId, String name, String cronExpression,
                             String timezone, String jobType) {
        return new Builder(tenantId, name, cronExpression, timezone, jobType);
    }

    public static final class Builder {
        private final String tenantId;
        private final String name;
        private final String cronExpression;
        private final String timezone;
        private final String jobType;
        private UUID id = UUID.randomUUID();
        private String queue = "default";
        private String payload = "{}";
        private int priority = 0;
        private int maxAttempts = 5;
        private boolean enabled = true;
        private MisfirePolicy misfirePolicy = MisfirePolicy.FIRE_ONCE;
        private int catchUpLimit = 100;

        private Builder(String tenantId, String name, String cronExpression,
                        String timezone, String jobType) {
            this.tenantId = tenantId;
            this.name = name;
            this.cronExpression = cronExpression;
            this.timezone = timezone;
            this.jobType = jobType;
        }

        public Builder id(UUID id) { this.id = id; return this; }
        public Builder queue(String queue) { this.queue = queue; return this; }
        public Builder payload(String payload) { this.payload = payload; return this; }
        public Builder priority(int priority) { this.priority = priority; return this; }
        public Builder maxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; return this; }
        public Builder enabled(boolean enabled) { this.enabled = enabled; return this; }
        public Builder misfirePolicy(MisfirePolicy policy) { this.misfirePolicy = policy; return this; }
        public Builder catchUpLimit(int limit) { this.catchUpLimit = limit; return this; }

        public CronSchedule build() {
            // Validate here rather than at the first tick: a typo in an expression should be
            // rejected by whoever is creating the schedule, while they are still looking at it.
            CronExpression.parse(cronExpression);
            java.time.ZoneId.of(timezone);
            return new CronSchedule(id, tenantId, name, cronExpression, timezone, jobType, queue,
                    payload, priority, maxAttempts, enabled, misfirePolicy, catchUpLimit,
                    null, null, 0L);
        }
    }

    /** The next fire instant for this schedule strictly after {@code after}. */
    public java.util.Optional<Instant> nextFireAfter(Instant after) {
        return CronExpression.parse(cronExpression)
                .nextAfter(java.time.ZonedDateTime.ofInstant(after, java.time.ZoneId.of(timezone)),
                        java.time.ZoneId.of(timezone))
                .map(java.time.ZonedDateTime::toInstant);
    }

    /** The idempotency key for one occurrence, and the reason a crashed ticker cannot double-fire.
     *
     * <p>It is derived purely from the schedule and the instant, so two tickers computing the same
     * occurrence produce the same key and the unique index on {@code (tenant_id, idempotency_key)}
     * rejects the second insert. No coordination, no leader required for correctness — leadership
     * is a load optimisation here, not a safety mechanism. */
    public String idempotencyKeyFor(Instant fireAt) {
        return "cron:" + id + ":" + fireAt.toEpochMilli();
    }
}
