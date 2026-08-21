package io.pendulum.core.bench;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.domain.Schedule;
import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Throughput and scheduling jitter. Not run by {@code mvn verify} — use {@code -Pbenchmarks},
 * because numbers from a shared CI runner are noise and publishing them would be worse than
 * publishing nothing.
 *
 * <h2>What is actually being measured</h2>
 * The handler is a no-op, so this measures <em>engine overhead</em> and nothing else: the cost of
 * claiming, marking running, and completing. Real jobs are dominated by their own I/O, so treat
 * this as the ceiling the engine imposes, not as a prediction of any real workload.
 *
 * <h2>Clock domains</h2>
 * Jitter is "how long after a job became eligible did it start running", which spans two clocks:
 * {@code run_at} is set by the database, the handler observes the JVM's. The offset between them
 * is measured once and subtracted. Without that correction the numbers would silently carry the
 * clock skew between the JVM and the Postgres container — which on a laptop is small, and in a
 * distributed setup is exactly the thing you must not assume away.
 */
class ThroughputBenchmark extends PostgresTestBase {

    private static final int JOB_COUNT = 4_000;
    private static final int WORKER_COUNT = 4;

    @Test
    @DisplayName("sustained throughput draining a saturated queue")
    void measure_throughput() {
        // The offset between the database clock and this JVM's clock, so jitter is measured
        // against a single timeline rather than two that merely look similar.
        Instant databaseNow = store.databaseNow();
        long clockOffsetMillis = databaseNow.toEpochMilli() - System.currentTimeMillis();

        // Every job becomes eligible at the same instant, so the run is a true thundering herd
        // rather than a trickle that hides queueing behaviour.
        Instant eligibleAt = databaseNow.plusSeconds(5);

        long enqueueStart = System.nanoTime();
        for (int i = 0; i < JOB_COUNT; i++) {
            store.enqueue(NewJob.of("bench", "noop")
                    .queue("bench")
                    .schedule(Schedule.at(eligibleAt))
                    .build());
        }
        double enqueueSeconds = (System.nanoTime() - enqueueStart) / 1e9;

        ConcurrentLinkedQueue<Long> jitterSamples = new ConcurrentLinkedQueue<>();
        AtomicLong lastCompletionMillis = new AtomicLong();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("noop", context -> {
            long nowOnDatabaseTimeline = System.currentTimeMillis() + clockOffsetMillis;
            jitterSamples.add(nowOnDatabaseTimeline - eligibleAt.toEpochMilli());
            lastCompletionMillis.set(nowOnDatabaseTimeline);
        });

        List<Worker> workers = new ArrayList<>();
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers.add(new Worker(store, handlers, RetryPolicy.defaults(),
                    WorkerConfig.defaults("bench")
                            .withWorkerId("bench-" + i)
                            // In-flight work across the fleet is kept near the connection pool
                            // size. Going wider does not go faster: a no-op job is two round
                            // trips, so past the pool the queueing simply moves from the engine
                            // into HikariCP and the number stops describing the engine at all.
                            .withConcurrency(8, 8)
                            .withPollInterval(Duration.ofMillis(10), Duration.ofMillis(100))));
        }

        try {
            workers.forEach(Worker::start);
            await().atMost(Duration.ofMinutes(5))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertThat(countInState("SUCCEEDED")).isEqualTo(JOB_COUNT));
        } finally {
            workers.forEach(Worker::close);
        }

        List<Long> jitter = new ArrayList<>(jitterSamples);
        jitter.sort(null);

        double wallSeconds = (lastCompletionMillis.get() - eligibleAt.toEpochMilli()) / 1000.0;
        double throughput = JOB_COUNT / wallSeconds;

        System.out.printf("""

                ============================================================
                 Pendulum throughput benchmark
                ============================================================
                 jobs                    %,d
                 workers                 %d (in-flight cap %d each)
                 connection pool         %d
                 handler                 no-op (measures engine overhead only)
                ------------------------------------------------------------
                 enqueue rate            %,.0f jobs/sec (%.2fs total)
                 drain wall time         %.2fs
                 THROUGHPUT              %,.0f jobs/sec
                ------------------------------------------------------------
                 queue delay while saturated, eligible -> handler start
                   p50                   %d ms
                   p99                   %d ms
                   max                   %d ms

                 NOT scheduling jitter. With every job eligible at once, this
                 is backlog drain: p50 necessarily lands near half the wall
                 time. Scheduling jitter is measured unsaturated, below.
                ============================================================
                %n""",
                JOB_COUNT, WORKER_COUNT, 8, DATA_SOURCE.getMaximumPoolSize(),
                JOB_COUNT / enqueueSeconds, enqueueSeconds,
                wallSeconds, throughput,
                percentile(jitter, 50), percentile(jitter, 99), jitter.getLast());

        assertThat(jitter).hasSize(JOB_COUNT);
        assertThat(countInState("SUCCEEDED")).isEqualTo(JOB_COUNT);
    }

    /**
     * Scheduling jitter proper: how promptly a due job starts when the engine is <em>not</em>
     * saturated. Jobs arrive at a steady trickle, so every measurement is the engine's own
     * responsiveness rather than a queue it is still working through.
     *
     * <p>The floor here is the poll interval, and that is the honest story: a polling dispatcher
     * cannot react faster than it polls. Tightening the interval trades database load for latency,
     * which is precisely the tradeoff {@code LISTEN/NOTIFY} would remove — a worker woken by the
     * enqueue itself starts in single-digit milliseconds without any idle polling at all.
     */
    @Test
    @DisplayName("scheduling jitter at low load, where it actually means something")
    void measure_scheduling_jitter_unsaturated() {
        int sampleCount = 120;
        Duration spacing = Duration.ofMillis(120);

        Instant databaseNow = store.databaseNow();
        long clockOffsetMillis = databaseNow.toEpochMilli() - System.currentTimeMillis();
        Instant firstDueAt = databaseNow.plusSeconds(3);

        List<Instant> dueTimes = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            Instant dueAt = firstDueAt.plus(spacing.multipliedBy(i));
            dueTimes.add(dueAt);
            store.enqueue(NewJob.of("bench", "punctual")
                    .queue("bench")
                    .schedule(Schedule.at(dueAt))
                    .build());
        }

        ConcurrentLinkedQueue<Long> jitterSamples = new ConcurrentLinkedQueue<>();
        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("punctual", context -> {
            long startedAt = System.currentTimeMillis() + clockOffsetMillis;
            jitterSamples.add(startedAt - context.job().runAt().toEpochMilli());
        });

        // Production defaults on purpose: 50ms when busy, backing off to 2s when idle. Reporting
        // a number obtained by tuning the poll interval down for the benchmark would be cheating.
        Worker worker = new Worker(store, handlers, RetryPolicy.defaults(),
                WorkerConfig.defaults("bench").withWorkerId("punctuality").withConcurrency(8, 8));

        try {
            worker.start();
            await().atMost(Duration.ofMinutes(2))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(countInState("SUCCEEDED")).isEqualTo(sampleCount));
        } finally {
            worker.close();
        }

        List<Long> jitter = new ArrayList<>(jitterSamples);
        jitter.sort(null);

        System.out.printf("""

                ============================================================
                 Pendulum scheduling jitter (unsaturated)
                ============================================================
                 samples                 %d, one every %d ms
                 workers                 1
                 poll interval           50 ms busy, backing off to 2s idle
                ------------------------------------------------------------
                 due -> handler start
                   p50                   %d ms
                   p95                   %d ms
                   p99                   %d ms
                   max                   %d ms

                 Bounded below by the poll interval. LISTEN/NOTIFY would
                 replace the floor with a push wake-up.
                ============================================================
                %n""",
                sampleCount, spacing.toMillis(),
                percentile(jitter, 50), percentile(jitter, 95),
                percentile(jitter, 99), jitter.getLast());

        assertThat(jitter).hasSize(sampleCount);
    }

    private static long percentile(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) return -1;
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
    }
}
