package io.pendulum.core.workflow;

import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.LeaseReaper;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The demo. A worker is SIGKILLed in the middle of a four-step workflow; another worker finishes
 * it, resuming at the step that was interrupted rather than starting again.
 *
 * <p>This is the difference between a job queue and a durable execution engine, and it is entirely
 * visible in one number: how many times step one ran.
 */
class WorkflowChaosIT extends PostgresTestBase {

    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final Duration HEARTBEAT = Duration.ofMillis(400);

    private final WorkflowStore workflows = new PostgresWorkflowStore(DATA_SOURCE);
    private WorkflowRegistry registry;
    private WorkflowEngine engine;
    private HandlerRegistry handlers;

    @BeforeEach
    void setUp() {
        execute("TRUNCATE TABLE workflow_runs CASCADE");
        registry = new WorkflowRegistry();
        engine = new WorkflowEngine(workflows, store, registry, DATA_SOURCE);
        handlers = new HandlerRegistry();
        engine.registerWith(handlers);
    }

    @Test
    @DisplayName("a workflow whose worker is killed mid-flight resumes at the interrupted step")
    void a_killed_worker_does_not_restart_the_workflow() throws Exception {
        AtomicInteger reserve = new AtomicInteger();
        AtomicInteger charge = new AtomicInteger();
        AtomicInteger ship = new AtomicInteger();
        AtomicInteger notify = new AtomicInteger();

        ConcurrentLinkedQueue<String> executions = new ConcurrentLinkedQueue<>();
        CountDownLatch reachedCharge = new CountDownLatch(1);
        CountDownLatch victimIsDead = new CountDownLatch(1);

        registry.register(Workflow.named("place-order")
                .step("reserve-stock", context -> {
                    executions.add("reserve-stock");
                    reserve.incrementAndGet();
                    return "{\"reservationId\":\"r-1\"}";
                })
                .step("charge-card", context -> {
                    executions.add("charge-card");
                    if (charge.incrementAndGet() == 1) {
                        // The first execution hangs here; the worker dies underneath it.
                        reachedCharge.countDown();
                        victimIsDead.await(30, TimeUnit.SECONDS);
                        Thread.sleep(Duration.ofMinutes(5));
                    }
                    return "{\"chargeId\":\"c-1\"}";
                })
                .step("ship", context -> {
                    executions.add("ship");
                    ship.incrementAndGet();
                    assertThat(context.requireOutputOf("charge-card")).contains("c-1");
                    return "{\"tracking\":\"t-1\"}";
                })
                .step("notify", context -> {
                    executions.add("notify");
                    notify.incrementAndGet();
                    return "{\"notified\":true}";
                })
                .build());

        UUID runId = engine.start("acme", "place-order", "{\"orderId\":42}");

        Worker victim = worker("victim");
        victim.start();
        assertThat(reachedCharge.await(30, TimeUnit.SECONDS)).as("reached the charge step").isTrue();

        // Step one is committed by now; the run is mid-way through step two.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> workflows.find(runId).orElseThrow().completedSteps() >= 1);

        victim.terminateAbruptly();
        victimIsDead.countDown();

        try (LeaseReaper reaper = new LeaseReaper(store, Duration.ofMillis(250), 100);
             Worker survivor = worker("survivor")) {
            reaper.start();
            survivor.start();

            await().atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> assertThat(workflows.find(runId).orElseThrow().state())
                            .isEqualTo("SUCCEEDED"));
        }

        assertThat(reserve.get())
                .as("the committed step is replayed, never re-executed")
                .isEqualTo(1);
        assertThat(charge.get())
                .as("the interrupted step runs again — its side effect was never committed")
                .isEqualTo(2);
        assertThat(ship.get()).isEqualTo(1);
        assertThat(notify.get()).isEqualTo(1);

        assertThat(workflows.stepResults(runId)).hasSize(4);
        assertThat(executions).startsWith("reserve-stock", "charge-card", "charge-card");
    }

    private Worker worker(String id) {
        return new Worker(store, handlers, RetryPolicy.defaults(),
                WorkerConfig.defaults("default")
                        .withWorkerId(id)
                        .withLease(LEASE, HEARTBEAT)
                        .withConcurrency(4, 8)
                        .withPollInterval(Duration.ofMillis(25), Duration.ofMillis(200))
                        .withDrainTimeout(Duration.ofSeconds(5)));
    }
}
