package io.pendulum.core.engine;

import io.pendulum.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Reclaims jobs whose owner stopped heartbeating.
 *
 * <p>This is the other half of the crash story. A worker that is SIGKILLed has no chance to
 * release anything, so recovery cannot depend on the worker doing anything at all: the lease
 * simply stops being renewed, and after {@code leaseDuration} the reaper takes the job back.
 * That is why the lease duration is really a recovery-time knob.
 *
 * <p>Safe to run on every worker rather than only the leader, because the reap statement uses
 * the same {@code SKIP LOCKED} discipline as dispatch — concurrent reapers partition the work
 * instead of fighting over it. In production it is still worth making this a leader-only
 * responsibility (Postgres advisory lock) to keep the scan count independent of fleet size.
 */
public final class LeaseReaper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    /** Ceiling on batches drained per tick, so one pass cannot run forever. */
    private static final int MAX_PASSES_PER_TICK = 50;

    private final JobStore store;
    private final Duration interval;
    private final int batchSize;
    private final LongAdder reclaimed = new LongAdder();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pendulum-reaper");
                thread.setDaemon(true);
                return thread;
            });

    public LeaseReaper(JobStore store, Duration interval, int batchSize) {
        this.store = store;
        this.interval = interval;
        this.batchSize = batchSize;
    }

    public static LeaseReaper defaults(JobStore store) {
        return new LeaseReaper(store, Duration.ofSeconds(1), 200);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long millis = interval.toMillis();
        // A jittered initial delay so a fleet restarted together does not reap in lockstep.
        long initialDelay = ThreadLocalRandom.current().nextLong(millis + 1);
        scheduler.scheduleWithFixedDelay(this::reapOnce, initialDelay, millis, TimeUnit.MILLISECONDS);
        log.info("lease reaper started (interval={}, batch={})", interval, batchSize);
    }

    /**
     * One reap pass, draining in batches so a large backlog of orphans (a whole node lost) is
     * cleared in one interval instead of {@code batchSize} jobs per tick.
     *
     * @return jobs reclaimed in this pass
     */
    public int reapOnce() {
        int total = 0;
        try {
            int batch;
            int passes = 0;
            do {
                batch = store.reapExpiredLeases(batchSize);
                total += batch;
                // Bounded, because "drain until a short batch" is unbounded when leases expire as
                // fast as they are reclaimed — a fleet-wide stall would pin this thread in the loop
                // forever and starve the schedule. Whatever is left waits for the next tick.
            } while (batch == batchSize && ++passes < MAX_PASSES_PER_TICK);

            if (passes >= MAX_PASSES_PER_TICK) {
                log.warn("reap pass hit its {}-batch ceiling with work still expired; "
                        + "leases are expiring faster than they are being reclaimed", MAX_PASSES_PER_TICK);
            }

            if (total > 0) {
                reclaimed.add(total);
                log.info("reclaimed {} expired lease(s)", total);
            }
        } catch (RuntimeException e) {
            // Never let a failed pass kill the schedule: scheduleWithFixedDelay cancels the task
            // permanently if it throws, and a silently dead reaper means orphaned jobs stop coming
            // back at all — a failure mode that looks like data loss.
            log.error("reap pass failed", e);
        }
        return total;
    }

    public long totalReclaimed() {
        return reclaimed.sum();
    }

    @Override
    public void close() {
        running.set(false);
        scheduler.shutdownNow();
    }
}
