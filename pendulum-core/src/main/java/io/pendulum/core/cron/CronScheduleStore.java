package io.pendulum.core.cron;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Persistence for cron schedules. */
public interface CronScheduleStore {

    /** Create or update by {@code (tenantId, name)}, setting the first fire time. */
    UUID save(CronSchedule schedule, Instant firstFireAt);

    /**
     * Claim schedules that are due, pushing their next fire time forward so a concurrent ticker
     * sees nothing. Returned schedules carry the <em>original</em> due instant in
     * {@link CronSchedule#dueAt()}.
     */
    List<CronSchedule> claimDue(int limit, Duration claimFor);

    /** Record the outcome of a tick: where the schedule fires next, and how many jobs it produced. */
    void recordFired(UUID id, Instant nextFireAt, int firedCount);

    /** Take a schedule out of service — used when its expression or zone no longer parses. */
    void disable(UUID id, String reason);

    Optional<CronSchedule> find(UUID id);

    List<CronSchedule> findAll(String tenantId);
}
