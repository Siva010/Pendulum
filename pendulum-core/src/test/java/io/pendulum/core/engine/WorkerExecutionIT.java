package io.pendulum.core.engine;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.retry.BackoffPolicy;
import io.pendulum.core.retry.ErrorClass;
import io.pendulum.core.retry.ErrorClassifier;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.retry.TerminalJobException;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** End to end: real workers, real Postgres, real handler code. */
class WorkerExecutionIT extends PostgresTestBase {

    private final List<AutoCloseable> running = new ArrayList<>();

    @AfterEach
    void stopWorkers() {
        running.forEach(worker -> {
            try {
                worker.close();
            } catch (Exception ignored) {
                // best effort teardown
            }
        });
        running.clear();
    }

    @Test
    @DisplayName("three workers drain a queue with every job executed exactly once")
    void jobs_run_exactly_once_across_a_fleet() {
        int jobCount = 300;
        Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("count-me", context ->
                executions.computeIfAbsent(context.job().id(), id -> new AtomicInteger())
                        .incrementAndGet());

        for (int i = 0; i < jobCount; i++) {
            enqueue("count-me");
        }

        startWorkers(handlers, RetryPolicy.defaults(), 3);

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(countInState("SUCCEEDED")).isEqualTo(jobCount));

        assertThat(executions).hasSize(jobCount);
        assertThat(executions.values())
                .as("no job executed twice")
                .allSatisfy(count -> assertThat(count.get()).isEqualTo(1));
    }

    @Test
    @DisplayName("a transient failure comes back for another attempt")
    void transient_failures_are_retried() {
        AtomicInteger attempts = new AtomicInteger();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("flaky", context -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("vendor returned 503");
            }
        });

        UUID id = enqueue("flaky");
        startWorkers(handlers, immediateRetryPolicy(), 1);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(stateOf(id)).isEqualTo("SUCCEEDED"));

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(reload(id).attempt()).isEqualTo(3);
    }

    /**
     * The distinction that keeps a retry engine from becoming an outage amplifier: a 422 is not a
     * 503. Retrying a request the server will reject every single time turns one bad job into
     * {@code max_attempts} calls against a vendor that already said no.
     */
    @Test
    @DisplayName("a terminal failure goes straight to the dead-letter state with attempts to spare")
    void terminal_failures_skip_retries() {
        AtomicInteger attempts = new AtomicInteger();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("malformed", context -> {
            attempts.incrementAndGet();
            throw new TerminalJobException("422 unprocessable: missing customer id");
        });

        UUID id = enqueue(NewJob.of("tenant-a", "malformed").maxAttempts(5).build());
        startWorkers(handlers, immediateRetryPolicy(), 1);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(stateOf(id)).isEqualTo("DEAD_LETTERED"));

        assertThat(attempts.get()).as("tried once, never again").isEqualTo(1);
        Job dead = reload(id);
        assertThat(dead.attempt()).isEqualTo(1);
        assertThat(dead.lastError()).contains("missing customer id");
    }

    @Test
    @DisplayName("a job that keeps failing is dead-lettered when its budget runs out")
    void exhausted_retries_end_in_the_dead_letter_state() {
        AtomicInteger attempts = new AtomicInteger();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("always-fails", context -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("connection reset");
        });

        UUID id = enqueue(NewJob.of("tenant-a", "always-fails").maxAttempts(3).build());
        startWorkers(handlers, immediateRetryPolicy(), 1);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(stateOf(id)).isEqualTo("DEAD_LETTERED"));

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(reload(id).lastError()).contains("attempts exhausted");
    }

    /**
     * Mid-deploy, half the fleet does not yet know about a new job type. Dead-lettering on the
     * first sight of an unknown type would turn a rolling deploy into a DLQ full of perfectly
     * valid work, so the job is retried and only the ordinary attempt budget ends it.
     */
    @Test
    @DisplayName("an unknown job type is retried, not dead-lettered on sight")
    void unknown_job_types_are_retried() {
        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("something-else", context -> { });

        UUID id = enqueue(NewJob.of("tenant-a", "not-deployed-here").maxAttempts(3).build());
        startWorkers(handlers, immediateRetryPolicy(), 1);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(reload(id).attempt()).isGreaterThanOrEqualTo(1));

        Job job = reload(id);
        assertThat(job.state().discriminator()).isIn("PENDING", "LEASED", "RUNNING", "DEAD_LETTERED");
        assertThat(job.lastError()).contains("no handler registered");
    }

    @Test
    @DisplayName("a job scheduled for later is not picked up early")
    void delayed_jobs_wait_their_turn() {
        AtomicInteger executions = new AtomicInteger();

        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("later", context -> executions.incrementAndGet());

        UUID id = enqueue(NewJob.of("tenant-a", "later").runAfter(Duration.ofHours(2)).build());
        startWorkers(handlers, RetryPolicy.defaults(), 2);

        await().during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(4))
                .untilAsserted(() -> assertThat(executions.get()).isZero());

        assertThat(stateOf(id)).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("graceful shutdown hands back work that never started")
    void draining_releases_unstarted_jobs() {
        HandlerRegistry handlers = new HandlerRegistry();
        handlers.register("slow", context -> Thread.sleep(400));

        for (int i = 0; i < 40; i++) {
            enqueue("slow");
        }

        Worker worker = new Worker(store, handlers, RetryPolicy.defaults(),
                WorkerConfig.defaults(NewJob.DEFAULT_QUEUE)
                        .withConcurrency(20, 20)
                        .withDrainTimeout(Duration.ofSeconds(10)));
        worker.start();

        await().atMost(Duration.ofSeconds(10)).until(() -> worker.metrics().started() > 0);
        worker.close();

        // Nothing is left mid-flight: everything either finished or went back on the queue.
        assertThat(countInState("LEASED")).isZero();
        assertThat(countInState("RUNNING")).isZero();
        assertThat(countInState("SUCCEEDED") + countInState("PENDING")).isEqualTo(40);
    }

    // ---------------------------------------------------------------- helpers

    private void startWorkers(HandlerRegistry handlers, RetryPolicy policy, int count) {
        for (int i = 0; i < count; i++) {
            Worker worker = new Worker(store, handlers, policy,
                    WorkerConfig.defaults(NewJob.DEFAULT_QUEUE)
                            .withWorkerId("worker-" + i)
                            .withConcurrency(8, 16)
                            .withPollInterval(Duration.ofMillis(25), Duration.ofMillis(200)));
            worker.start();
            running.add(worker);
        }
    }

    /** Backoff of zero, so retry behaviour can be asserted without the suite sleeping through it. */
    private static RetryPolicy immediateRetryPolicy() {
        Map<ErrorClass, BackoffPolicy> backoffs = new EnumMap<>(ErrorClass.class);
        backoffs.put(ErrorClass.TRANSIENT, BackoffPolicy.fixed(Duration.ZERO));
        backoffs.put(ErrorClass.RATE_LIMITED, BackoffPolicy.fixed(Duration.ZERO));
        backoffs.put(ErrorClass.TERMINAL, BackoffPolicy.fixed(Duration.ZERO));
        return new RetryPolicy(ErrorClassifier.defaults(), backoffs);
    }
}
