package io.pendulum.server;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.pendulum.core.cron.CronScheduler;
import io.pendulum.core.engine.LeaseReaper;
import io.pendulum.core.engine.WorkerMetrics;
import io.pendulum.core.outbox.OutboxRelay;
import io.pendulum.core.outbox.OutboxStore;
import io.pendulum.core.store.JobStore;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/**
 * Binds the engine's counters into Micrometer, exposing them at
 * {@code /actuator/prometheus}.
 *
 * <p>This class exists so that {@code pendulum-core} does not. The engine counts with plain
 * {@link java.util.concurrent.atomic.LongAdder}s and knows nothing about Micrometer, which is what
 * lets it be embedded or unit-tested without a metrics registry on the classpath. Translation
 * happens here, at the edge, exactly like the Spring wiring does.
 *
 * <h2>The metric worth alerting on</h2>
 * {@code pendulum_fenced_writes_total}. A worker producing a result for a job it no longer owns is
 * the lease race actually happening — harmless, because the database rejects the write, but it
 * means leases are expiring under live work. Non-zero in steady state says the lease duration or
 * the heartbeat interval is wrong, and it is the only signal that says so before users notice
 * duplicated effects.
 *
 * <p>{@code pendulum_queue_depth} is the autoscaling signal. CPU is the wrong one for a queue
 * consumer: a worker blocked on a slow vendor API is idle by CPU and desperately behind by backlog.
 */
@Component
public class PendulumMetrics {

    /**
     * Queue depth needs a {@code GROUP BY} against the jobs table, and Prometheus scrapes several
     * gauges at once. Without a short cache, one scrape becomes six aggregate queries against the
     * hottest table in the schema — monitoring that degrades the thing it measures.
     */
    private static final Duration DEPTH_CACHE_TTL = Duration.ofSeconds(5);

    private final AtomicReference<CachedDepth> cachedDepth = new AtomicReference<>(CachedDepth.empty());

    public PendulumMetrics(MeterRegistry registry,
                           WorkerPool workers,
                           LeaseReaper reaper,
                           OutboxRelay outboxRelay,
                           OutboxStore outbox,
                           CronScheduler cron,
                           JobStore jobs,
                           PendulumProperties properties) {

        // Monotonic engine counters. FunctionCounter rather than Counter because the engine owns
        // the value and Micrometer only reads it — there is no second copy to drift.
        counter(registry, "pendulum.jobs.claimed", workers, WorkerMetrics.Snapshot::claimed,
                "Jobs claimed from the queue");
        counter(registry, "pendulum.jobs.started", workers, WorkerMetrics.Snapshot::started,
                "Jobs whose handler began executing");
        counter(registry, "pendulum.jobs.succeeded", workers, WorkerMetrics.Snapshot::succeeded,
                "Jobs completed successfully");
        counter(registry, "pendulum.jobs.retried", workers, WorkerMetrics.Snapshot::retried,
                "Job attempts that failed and were rescheduled");
        counter(registry, "pendulum.jobs.dead_lettered", workers, WorkerMetrics.Snapshot::deadLettered,
                "Jobs that exhausted their attempts or failed terminally");
        counter(registry, "pendulum.leases.lost", workers, WorkerMetrics.Snapshot::leasesLost,
                "In-flight jobs whose lease was reassigned mid-execution");
        counter(registry, "pendulum.writes.fenced", workers, WorkerMetrics.Snapshot::fencedWrites,
                "Result writes rejected because the lease had moved on — watch this one");
        counter(registry, "pendulum.polls.empty", workers, WorkerMetrics.Snapshot::emptyPolls,
                "Dispatch polls that found nothing, the cost of polling");

        Gauge.builder("pendulum.workers.inflight", workers, WorkerPool::inFlight)
                .description("Jobs currently executing across this process's workers")
                .register(registry);

        Gauge.builder("pendulum.workers.count", workers, pool -> pool.size())
                .description("Workers in this process")
                .register(registry);

        FunctionCounter.builder("pendulum.leases.reclaimed", reaper, LeaseReaper::totalReclaimed)
                .description("Expired leases reclaimed by the reaper")
                .register(registry);

        FunctionCounter.builder("pendulum.outbox.published", outboxRelay, OutboxRelay::totalPublished)
                .description("Outbox messages delivered")
                .register(registry);
        FunctionCounter.builder("pendulum.outbox.failed", outboxRelay, OutboxRelay::totalFailed)
                .description("Outbox delivery attempts that failed")
                .register(registry);
        Gauge.builder("pendulum.outbox.pending", outbox, store -> store.countInState("PENDING"))
                .description("Outbox messages awaiting delivery")
                .register(registry);

        FunctionCounter.builder("pendulum.cron.fired", cron, CronScheduler::totalFired)
                .description("Jobs enqueued by cron schedules")
                .register(registry);
        FunctionCounter.builder("pendulum.cron.skipped", cron, CronScheduler::totalSkipped)
                .description("Cron occurrences skipped by misfire policy")
                .register(registry);

        // The signal a worker pool should actually be autoscaled on.
        for (String state : new String[]{"PENDING", "LEASED", "RUNNING", "DEAD_LETTERED"}) {
            Gauge.builder("pendulum.queue.depth", state, s -> depth(jobs, properties.queue()).getOrDefault(s, 0L))
                    .description("Jobs in this state on the configured queue")
                    .tag("state", state)
                    .tag("queue", properties.queue())
                    .register(registry);
        }
    }

    private static void counter(MeterRegistry registry, String name, WorkerPool workers,
                                ToDoubleFunction<WorkerMetrics.Snapshot> reader, String description) {
        FunctionCounter.builder(name, workers,
                        pool -> reader.applyAsDouble(pool.aggregateMetrics()))
                .description(description)
                .register(registry);
    }

    private Map<String, Long> depth(JobStore jobs, String queue) {
        CachedDepth current = cachedDepth.get();
        if (!current.isStale()) {
            return current.counts();
        }
        Map<String, Long> fresh = jobs.queueDepth(queue);
        cachedDepth.set(new CachedDepth(fresh, System.nanoTime()));
        return fresh;
    }

    private record CachedDepth(Map<String, Long> counts, long readAtNanos) {
        static CachedDepth empty() {
            return new CachedDepth(Map.of(), 0L);
        }

        boolean isStale() {
            return System.nanoTime() - readAtNanos > DEPTH_CACHE_TTL.toNanos();
        }
    }
}
