package io.pendulum.core.cron;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.domain.Schedule;
import io.pendulum.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Turns cron schedules into ordinary jobs.
 *
 * <p>It fires nothing itself. Every occurrence becomes a row in {@code jobs}, so scheduled work
 * inherits leasing, fencing, retries, the attempt budget and the dead-letter queue unchanged. A
 * scheduler that executed work directly would have to reimplement all of that, and would get the
 * lease race wrong on the first try.
 *
 * <h2>Why this is safe without a leader</h2>
 * Every occurrence carries a deterministic idempotency key — {@code cron:<schedule>:<instant>} —
 * so two tickers computing the same occurrence produce the same key, and the unique index on
 * {@code (tenant_id, idempotency_key)} rejects the second insert. A ticker that fires and dies
 * before recording its progress re-fires the same occurrence on recovery and the database absorbs
 * it. Running this on one node via leader election is a load optimisation, not a safety
 * requirement, and knowing which of the two it is matters: a correctness mechanism that depends on
 * an election is a correctness mechanism that fails during one.
 */
public final class CronScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CronScheduler.class);

    private final CronScheduleStore schedules;
    private final JobStore jobs;
    private final Duration tickInterval;
    private final Duration claimFor;
    private final int batchSize;

    private final LongAdder fired = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ScheduledExecutorService ticker =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pendulum-cron");
                thread.setDaemon(true);
                return thread;
            });

    public CronScheduler(CronScheduleStore schedules, JobStore jobs,
                         Duration tickInterval, Duration claimFor, int batchSize) {
        if (claimFor.compareTo(tickInterval) <= 0) {
            throw new IllegalArgumentException(
                    "claimFor (" + claimFor + ") must exceed tickInterval (" + tickInterval + ")");
        }
        this.schedules = schedules;
        this.jobs = jobs;
        this.tickInterval = tickInterval;
        this.claimFor = claimFor;
        this.batchSize = batchSize;
    }

    public static CronScheduler defaults(CronScheduleStore schedules, JobStore jobs) {
        return new CronScheduler(schedules, jobs, Duration.ofSeconds(5), Duration.ofSeconds(30), 100);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long millis = tickInterval.toMillis();
        ticker.scheduleWithFixedDelay(this::tick,
                ThreadLocalRandom.current().nextLong(millis + 1), millis, TimeUnit.MILLISECONDS);
        log.info("cron scheduler started (tick={}, batch={})", tickInterval, batchSize);
    }

    /**
     * One pass.
     *
     * @return how many jobs were enqueued
     */
    public int tick() {
        int enqueued = 0;
        try {
            Instant now = jobs.databaseNow();
            for (CronSchedule schedule : schedules.claimDue(batchSize, claimFor)) {
                enqueued += fire(schedule, now);
            }
        } catch (RuntimeException e) {
            // scheduleWithFixedDelay cancels the task for good if it throws, and a dead cron ticker
            // is invisible: nothing errors, jobs simply stop appearing.
            log.error("cron tick failed", e);
        }
        return enqueued;
    }

    private int fire(CronSchedule schedule, Instant now) {
        CronExpression expression;
        ZoneId zone;
        try {
            expression = CronExpression.parse(schedule.cronExpression());
            zone = ZoneId.of(schedule.timezone());
        } catch (RuntimeException e) {
            // A schedule that cannot be parsed will never parse. Retrying it every tick forever
            // just fills the log, so take it out of service with the reason recorded where an
            // operator will find it.
            log.error("disabling cron schedule '{}' ({}): {}", schedule.name(), schedule.id(), e.toString());
            schedules.disable(schedule.id(), "invalid schedule: " + e.getMessage());
            return 0;
        }

        List<Instant> due = occurrencesDue(expression, zone, schedule, now);
        List<Instant> toFire = switch (schedule.misfirePolicy()) {
            case SKIP -> List.of();
            case FIRE_ONCE -> due.isEmpty() ? List.of() : List.of(due.getLast());
            case FIRE_ALL -> due;
        };

        if (due.size() > toFire.size()) {
            skipped.add(due.size() - (long) toFire.size());
            log.info("cron '{}' missed {} occurrence(s), firing {} under policy {}",
                    schedule.name(), due.size(), toFire.size(), schedule.misfirePolicy());
        }

        int enqueued = 0;
        for (Instant fireAt : toFire) {
            if (enqueueOccurrence(schedule, fireAt)) {
                enqueued++;
            }
        }

        Instant next = expression.nextAfter(ZonedDateTime.ofInstant(now, zone), zone)
                .map(ZonedDateTime::toInstant)
                .orElse(null);

        if (next == null) {
            log.warn("cron schedule '{}' has no further occurrences; disabling", schedule.name());
            schedules.disable(schedule.id(), "no further occurrences");
            return enqueued;
        }

        schedules.recordFired(schedule.id(), next, enqueued);
        fired.add(enqueued);
        return enqueued;
    }

    /**
     * Every occurrence from the claimed due instant up to now, inclusive of the due one.
     *
     * <p>In steady state this is a single element: the occurrence that just came due. It only grows
     * when nothing ticked for a while, which is precisely the misfire case.
     */
    private static List<Instant> occurrencesDue(CronExpression expression, ZoneId zone,
                                                CronSchedule schedule, Instant now) {
        List<Instant> occurrences = new ArrayList<>();
        if (schedule.dueAt() == null || schedule.dueAt().isAfter(now)) {
            return occurrences;
        }
        occurrences.add(schedule.dueAt());

        ZonedDateTime cursor = ZonedDateTime.ofInstant(schedule.dueAt(), zone);
        while (occurrences.size() < schedule.catchUpLimit()) {
            ZonedDateTime next = expression.nextAfter(cursor, zone).orElse(null);
            if (next == null || next.toInstant().isAfter(now)) {
                break;
            }
            occurrences.add(next.toInstant());
            cursor = next;
        }
        return occurrences;
    }

    /**
     * @return true when this occurrence created a job; false when the idempotency key already
     *         existed, meaning some other ticker (or an earlier crashed attempt of this one)
     *         already fired it
     */
    private boolean enqueueOccurrence(CronSchedule schedule, Instant fireAt) {
        NewJob job = NewJob.of(schedule.tenantId(), schedule.jobType())
                .queue(schedule.queue())
                .payload(schedule.payload())
                .priority(schedule.priority())
                .maxAttempts(schedule.maxAttempts())
                .idempotencyKey(schedule.idempotencyKeyFor(fireAt))
                // The occurrence's own instant, not now(). A catch-up run for 02:00 is still the
                // 02:00 run, and a handler that reads run_at to decide what window to process must
                // see the window it is meant to process.
                .schedule(Schedule.at(fireAt))
                .build();

        JobStore.EnqueueResult result = jobs.enqueue(job);
        if (!result.created()) {
            log.debug("cron '{}' occurrence at {} was already enqueued", schedule.name(), fireAt);
        }
        return result.created();
    }

    public long totalFired() {
        return fired.sum();
    }

    public long totalSkipped() {
        return skipped.sum();
    }

    @Override
    public void close() {
        running.set(false);
        ticker.shutdownNow();
    }
}
