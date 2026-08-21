package io.pendulum.core.bench;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.JobHandler;
import io.pendulum.core.engine.LeaseReaper;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * How long a fleet takes to recover from losing a worker, and how many jobs get replayed when it
 * happens. Run with {@code -Pbenchmarks}.
 *
 * <p>The headline number here is mostly a restatement of the lease duration, and saying so is the
 * point: recovery time is a knob, not an achievement. A two-second lease recovers in about two
 * seconds and tolerates almost no GC pause; a thirty-second lease survives a brutal stop-the-world
 * and makes a crashed worker's jobs wait half a minute. The engine does not remove that tradeoff,
 * it just makes it explicit and safe to tune — the fencing token is what stops a short lease from
 * turning into double execution.
 */
class RecoveryBenchmark extends PostgresTestBase {

    private static final int JOB_COUNT = 400;
    private static final Duration LEASE = Duration.ofSeconds(5);
    private static final Duration HEARTBEAT = Duration.ofSeconds(1);

    @Test
    @DisplayName("recovery time and replay count after a worker is killed mid-flight")
    void measure_recovery_after_a_kill() throws Exception {
        Set<UUID> applied = ConcurrentHashMap.newKeySet();
        AtomicInteger replays = new AtomicInteger();
        CountDownLatch victimIsBusy = new CountDownLatch(10);

        JobHandler handler = context -> {
            Thread.sleep(25);
            if (!applied.add(context.job().id())) {
                replays.incrementAndGet();
            }
        };

        HandlerRegistry victimHandlers = new HandlerRegistry();
        victimHandlers.register("recover", context -> {
            victimIsBusy.countDown();
            handler.handle(context);
        });

        HandlerRegistry survivorHandlers = new HandlerRegistry();
        survivorHandlers.register("recover", handler);

        for (int i = 0; i < JOB_COUNT; i++) {
            store.enqueue(NewJob.of("bench", "recover").queue("bench").build());
        }

        Worker victim = worker("victim", victimHandlers);
        long killedAt;
        int strandedAtKill;

        try (LeaseReaper reaper = new LeaseReaper(store, Duration.ofMillis(250), 200);
             Worker survivor = worker("survivor", survivorHandlers)) {

            reaper.start();
            victim.start();
            survivor.start();

            assertThat(victimIsBusy.await(60, TimeUnit.SECONDS)).isTrue();

            strandedAtKill = victim.inFlightCount();
            killedAt = System.nanoTime();
            victim.terminateAbruptly();

            await().atMost(Duration.ofMinutes(3))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> assertThat(countInState("SUCCEEDED")).isEqualTo(JOB_COUNT));
        }

        double recoverySeconds = (System.nanoTime() - killedAt) / 1e9;

        System.out.printf("""

                ============================================================
                 Pendulum recovery benchmark
                ============================================================
                 jobs                    %,d
                 lease duration          %s   (the dominant term below)
                 heartbeat interval      %s
                 reaper interval         250 ms
                ------------------------------------------------------------
                 jobs stranded by kill   %d
                 kill -> queue fully drained
                                         %.2fs
                ------------------------------------------------------------
                 jobs lost               %d
                 jobs dead-lettered      %d
                 handler replays         %d   (at-least-once; bounded by the
                                              victim's in-flight window)
                ============================================================
                %n""",
                JOB_COUNT, LEASE, HEARTBEAT,
                strandedAtKill, recoverySeconds,
                JOB_COUNT - applied.size(), countInState("DEAD_LETTERED"), replays.get());

        assertThat(applied).as("no job lost").hasSize(JOB_COUNT);
        assertThat(countInState("DEAD_LETTERED")).isZero();
    }

    private Worker worker(String id, HandlerRegistry handlers) {
        return new Worker(store, handlers, RetryPolicy.defaults(),
                WorkerConfig.defaults("bench")
                        .withWorkerId(id)
                        .withLease(LEASE, HEARTBEAT)
                        .withConcurrency(16, 16)
                        .withPollInterval(Duration.ofMillis(10), Duration.ofMillis(100))
                        .withDrainTimeout(Duration.ofSeconds(10)));
    }
}
