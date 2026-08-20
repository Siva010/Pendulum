package io.pendulum.core.engine;

import java.util.concurrent.atomic.LongAdder;

/**
 * In-process counters. Deliberately not Micrometer: {@code pendulum-core} stays free of
 * observability dependencies, and {@code pendulum-server} binds these into Micrometer at the
 * edge. Keeping the core free of that coupling is what lets the engine be embedded or tested
 * without a metrics registry on the classpath.
 *
 * <p>{@link LongAdder} rather than {@code AtomicLong} because these are write-heavy and
 * read-rarely under contention from every execution thread, which is exactly the case where
 * striped counters beat a single CAS target.
 */
public final class WorkerMetrics {

    private final LongAdder claimed = new LongAdder();
    private final LongAdder started = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder retried = new LongAdder();
    private final LongAdder deadLettered = new LongAdder();
    private final LongAdder leasesLost = new LongAdder();
    private final LongAdder fencedWrites = new LongAdder();
    private final LongAdder emptyPolls = new LongAdder();

    void recordClaimed(int n) { claimed.add(n); }
    void recordStarted() { started.increment(); }
    void recordSucceeded() { succeeded.increment(); }
    void recordRetried() { retried.increment(); }
    void recordDeadLettered() { deadLettered.increment(); }
    void recordLeaseLost() { leasesLost.increment(); }
    void recordFencedWrite() { fencedWrites.increment(); }
    void recordEmptyPoll() { emptyPolls.increment(); }

    public Snapshot snapshot() {
        return new Snapshot(claimed.sum(), started.sum(), succeeded.sum(), retried.sum(),
                deadLettered.sum(), leasesLost.sum(), fencedWrites.sum(), emptyPolls.sum());
    }

    /**
     * @param fencedWrites completions rejected by the database because the lease had moved on.
     *                     This is the number to watch: a non-zero value in steady state means
     *                     leases are expiring under live work, and the lease duration or the
     *                     heartbeat interval is wrong.
     */
    public record Snapshot(
            long claimed,
            long started,
            long succeeded,
            long retried,
            long deadLettered,
            long leasesLost,
            long fencedWrites,
            long emptyPolls
    ) {
        public long settled() { return succeeded + retried + deadLettered; }
    }
}
