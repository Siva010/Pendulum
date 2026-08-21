package io.pendulum.core.workflow;

import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.BackoffPolicy;
import io.pendulum.core.retry.ErrorClass;
import io.pendulum.core.retry.ErrorClassifier;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.support.PostgresTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Durable execution: a workflow that fails partway resumes at the last completed step instead of
 * starting over.
 *
 * <p>The assertion that carries the whole feature is the execution <em>count</em> per step. Any
 * engine can make a workflow eventually succeed by retrying it from the beginning; the point of
 * persisting step outputs is that step one runs exactly once even when step two fails four times.
 */
class WorkflowIT extends PostgresTestBase {

    private final WorkflowStore workflows = new PostgresWorkflowStore(DATA_SOURCE);
    private final List<Worker> running = new ArrayList<>();

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

    @AfterEach
    void stopWorkers() {
        running.forEach(Worker::close);
        running.clear();
    }

    @Test
    @DisplayName("every step runs in order and later steps see earlier outputs")
    void steps_run_in_order_with_outputs_available() {
        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();

        registry.register(Workflow.named("onboard")
                .step("create-account", context -> {
                    order.add("create-account");
                    return "{\"accountId\":\"acct-1\"}";
                })
                .step("charge-fee", context -> {
                    order.add("charge-fee");
                    assertThat(context.requireOutputOf("create-account")).contains("acct-1");
                    return "{\"charged\":true}";
                })
                .step("send-welcome", context -> {
                    order.add("send-welcome");
                    assertThat(context.outputOf("charge-fee")).isPresent();
                    return "{\"sent\":true}";
                })
                .build());

        UUID runId = engine.start("acme", "onboard", "{\"email\":\"a@b.c\"}");
        startWorker();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(runState(runId)).isEqualTo("SUCCEEDED"));

        assertThat(order).containsExactly("create-account", "charge-fee", "send-welcome");
        assertThat(workflows.stepResults(runId)).hasSize(3);
        assertThat(workflows.find(runId).orElseThrow().result()).contains("sent");
    }

    /**
     * The headline behaviour. Step two fails twice; step one must still have executed exactly once
     * across all three attempts. Without persisted step outputs this count would be three, and the
     * account would have been created three times.
     */
    @Test
    @DisplayName("a retried workflow resumes at the failed step and does not re-run completed ones")
    void retry_resumes_rather_than_restarting() {
        AtomicInteger stepOne = new AtomicInteger();
        AtomicInteger stepTwo = new AtomicInteger();
        AtomicInteger stepThree = new AtomicInteger();

        registry.register(Workflow.named("flaky")
                .step("create-account", context -> {
                    stepOne.incrementAndGet();
                    return "{\"accountId\":\"acct-1\"}";
                })
                .step("call-vendor", context -> {
                    if (stepTwo.incrementAndGet() < 3) {
                        throw new IllegalStateException("vendor returned 503");
                    }
                    return "{\"vendorRef\":\"v-9\"}";
                })
                .step("finalise", context -> {
                    stepThree.incrementAndGet();
                    return "{\"done\":true}";
                })
                .build());

        UUID runId = engine.start("acme", "flaky", "{}");
        startWorker();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(runState(runId)).isEqualTo("SUCCEEDED"));

        assertThat(stepOne.get()).as("completed step never re-runs").isEqualTo(1);
        assertThat(stepTwo.get()).as("only the failing step retries").isEqualTo(3);
        assertThat(stepThree.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("a resumed run replays committed outputs, not recomputed ones")
    void resume_replays_committed_outputs() {
        AtomicInteger counter = new AtomicInteger();
        ConcurrentLinkedQueue<String> seenByStepTwo = new ConcurrentLinkedQueue<>();

        registry.register(Workflow.named("replay")
                .step("generate-id", context ->
                        // A different value every execution. If step two ever sees anything other
                        // than the first one, the run recomputed rather than replayed.
                        "{\"id\":" + counter.incrementAndGet() + "}")
                .step("use-id", context -> {
                    seenByStepTwo.add(context.requireOutputOf("generate-id"));
                    if (seenByStepTwo.size() < 2) {
                        throw new IllegalStateException("failing once to force a resume");
                    }
                    return "{\"ok\":true}";
                })
                .build());

        UUID runId = engine.start("acme", "replay", "{}");
        startWorker();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(runState(runId)).isEqualTo("SUCCEEDED"));

        assertThat(counter.get()).as("generated once").isEqualTo(1);
        assertThat(seenByStepTwo).hasSize(2);
        // Asserted as "both executions saw the identical value" rather than against a literal, so
        // the test pins the property and not Postgres' jsonb formatting.
        assertThat(seenByStepTwo.stream().distinct().toList())
                .as("the same committed value both times").hasSize(1);
        assertThat(seenByStepTwo.peek()).contains("1");
    }

    @Test
    @DisplayName("a workflow whose step never succeeds ends with its job dead-lettered")
    void exhausted_workflow_dead_letters_its_job() {
        registry.register(Workflow.named("doomed")
                .step("ok", context -> "{}")
                .step("never-works", context -> {
                    throw new IllegalStateException("permanently broken");
                })
                .build());

        UUID runId = engine.start("acme", "doomed", "{}");
        startWorker();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(countInState("DEAD_LETTERED")).isEqualTo(1));

        // Step one stays committed: the run is resumable if an operator replays the job after
        // fixing whatever broke, which is the entire reason to keep the partial history.
        assertThat(workflows.stepResults(runId)).hasSize(1);
        assertThat(workflows.find(runId).orElseThrow().completedSteps()).isEqualTo(1);
    }

    @Test
    @DisplayName("starting a workflow in a rolled-back transaction leaves no run and no job")
    void start_is_transactional() throws Exception {
        registry.register(Workflow.named("tx").step("only", context -> "{}").build());

        UUID runId;
        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            runId = engine.start(connection, "acme", "tx", "{}");
            connection.rollback();
        }

        assertThat(workflows.find(runId)).isEmpty();
        assertThat(countInState("PENDING")).isZero();
    }

    /**
     * Two executions of the same step race to commit. First writer wins and the loser adopts the
     * winner's value, so the run has one authoritative history rather than one that depends on
     * whichever worker finished last.
     */
    @Test
    @DisplayName("a duplicate step commit converges on the first writer's output")
    void duplicate_step_commits_converge() throws Exception {
        registry.register(Workflow.named("race").step("only", context -> "{}").build());

        UUID runId;
        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            runId = engine.start(connection, "acme", "race", "{}");
            connection.commit();
        }

        String first = workflows.recordStep(runId, 0, "only", "{\"winner\":\"A\"}", 1);
        String second = workflows.recordStep(runId, 0, "only", "{\"winner\":\"B\"}", 1);

        assertThat(first).contains("A");
        assertThat(second).as("B adopts A's committed output").contains("A");
        assertThat(workflows.stepResults(runId)).hasSize(1);
    }

    @Test
    @DisplayName("the resume point never moves backwards")
    void progress_is_monotonic() throws Exception {
        registry.register(Workflow.named("mono")
                .step("a", context -> "{}").step("b", context -> "{}").step("c", context -> "{}")
                .build());

        UUID runId;
        try (Connection connection = DATA_SOURCE.getConnection()) {
            connection.setAutoCommit(false);
            runId = engine.start(connection, "acme", "mono", "{}");
            connection.commit();
        }

        workflows.advanceTo(runId, 3);
        // A straggler finishing an earlier step must not rewind the run and cause c to run again.
        workflows.advanceTo(runId, 1);

        assertThat(workflows.find(runId).orElseThrow().completedSteps()).isEqualTo(3);
    }

    @Test
    @DisplayName("a workflow definition rejects duplicate step names")
    void duplicate_step_names_are_rejected() {
        assertThatThrownBy(() -> Workflow.named("bad")
                .step("same", context -> null)
                .step("same", context -> null)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate step name");

        assertDoesNotThrow(() -> Workflow.named("fine")
                .step("one", context -> null)
                .step("two", context -> null)
                .build());
    }

    @Test
    @DisplayName("starting an unregistered workflow fails immediately")
    void unknown_workflow_type_is_rejected() {
        assertThatThrownBy(() -> engine.start("acme", "does-not-exist", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no workflow registered");
    }

    // ---------------------------------------------------------------- helpers

    private String runState(UUID runId) {
        return workflows.find(runId).map(WorkflowRun::state).orElse("MISSING");
    }

    private void startWorker() {
        Map<ErrorClass, BackoffPolicy> backoffs = new EnumMap<>(ErrorClass.class);
        backoffs.put(ErrorClass.TRANSIENT, BackoffPolicy.fixed(Duration.ZERO));
        backoffs.put(ErrorClass.RATE_LIMITED, BackoffPolicy.fixed(Duration.ZERO));
        backoffs.put(ErrorClass.TERMINAL, BackoffPolicy.fixed(Duration.ZERO));

        Worker worker = new Worker(store, handlers,
                new RetryPolicy(ErrorClassifier.defaults(), backoffs),
                WorkerConfig.defaults("default")
                        .withWorkerId("workflow-worker")
                        .withConcurrency(4, 8)
                        .withPollInterval(Duration.ofMillis(20), Duration.ofMillis(100)));
        worker.start();
        running.add(worker);
    }
}
