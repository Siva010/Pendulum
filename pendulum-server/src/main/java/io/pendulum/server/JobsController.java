package io.pendulum.server;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.domain.Schedule;
import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerMetrics;
import io.pendulum.core.outbox.OutboxStore;
import io.pendulum.core.store.JobQuery;
import io.pendulum.core.store.JobStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** The admin and enqueue surface. Deliberately thin: it translates HTTP into engine calls. */
@RestController
@RequestMapping("/api")
public class JobsController {

    private final JobStore store;
    private final WorkerPool workers;
    private final OutboxStore outbox;

    public JobsController(JobStore store, WorkerPool workers, OutboxStore outbox) {
        this.store = store;
        this.workers = workers;
        this.outbox = outbox;
    }

    /**
     * @param idempotencyKey when supplied, enqueueing the same key twice returns the original job
     *                       and creates nothing. This is what makes a client-side retry of the
     *                       enqueue itself safe — the network failing after the insert commits is
     *                       otherwise indistinguishable from it failing before.
     */
    public record EnqueueRequest(
            String tenantId,
            String jobType,
            String queue,
            Object payload,
            Integer priority,
            Integer maxAttempts,
            String idempotencyKey,
            Instant runAt,
            Long delaySeconds
    ) {}

    public record JobView(
            UUID id,
            String tenantId,
            String queue,
            String jobType,
            String state,
            int priority,
            int attempt,
            int maxAttempts,
            Instant runAt,
            String lastError,
            String leaseOwner,
            Instant leaseExpiresAt,
            int replayCount,
            Instant createdAt,
            Instant updatedAt
    ) {
        static JobView of(Job job) {
            String owner = switch (job.state()) {
                case io.pendulum.core.domain.JobState.Leased leased -> leased.owner();
                case io.pendulum.core.domain.JobState.Running running -> running.owner();
                default -> null;
            };
            Instant expiry = switch (job.state()) {
                case io.pendulum.core.domain.JobState.Leased leased -> leased.expiresAt();
                case io.pendulum.core.domain.JobState.Running running -> running.expiresAt();
                default -> null;
            };
            return new JobView(job.id(), job.tenantId(), job.queue(), job.jobType(),
                    job.state().discriminator(), job.priority(), job.attempt(), job.maxAttempts(),
                    job.runAt(), job.lastError(), owner, expiry, job.replayCount(),
                    job.createdAt(), job.updatedAt());
        }
    }

    @PostMapping("/jobs")
    public ResponseEntity<Map<String, Object>> enqueue(@RequestBody EnqueueRequest request) {
        NewJob.Builder builder = NewJob.of(request.tenantId(), request.jobType());

        if (request.queue() != null) builder.queue(request.queue());
        if (request.priority() != null) builder.priority(request.priority());
        if (request.maxAttempts() != null) builder.maxAttempts(request.maxAttempts());
        if (request.idempotencyKey() != null) builder.idempotencyKey(request.idempotencyKey());
        if (request.payload() != null) builder.payload(JsonPayloads.toJson(request.payload()));

        if (request.runAt() != null) {
            builder.schedule(Schedule.at(request.runAt()));
        } else if (request.delaySeconds() != null) {
            builder.schedule(Schedule.after(Duration.ofSeconds(request.delaySeconds())));
        }

        JobStore.EnqueueResult result = store.enqueue(builder.build());

        // 201 for a new job, 200 for one an idempotency key deduplicated. The distinction matters
        // to a caller retrying an enqueue: both are success, but only one created work.
        return ResponseEntity
                .status(result.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(Map.of("id", result.id(), "created", result.created()));
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobView> get(@PathVariable UUID id) {
        return store.find(id)
                .map(JobView::of)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Queue depth by state — the signal a worker pool should be autoscaled on. CPU is the wrong
     * signal for a queue consumer: a worker blocked on a slow vendor API is idle by CPU and
     * desperately behind by backlog.
     */
    @GetMapping("/queues/{queue}/depth")
    public Map<String, Long> depth(@PathVariable String queue) {
        return store.queueDepth(queue);
    }

    /** Filtered, paged job listing — the backbone of the admin console. */
    @GetMapping("/jobs")
    public Map<String, Object> list(@RequestParam(required = false) String tenantId,
                                    @RequestParam(required = false) String queue,
                                    @RequestParam(required = false) String state,
                                    @RequestParam(required = false) String jobType,
                                    @RequestParam(defaultValue = "50") int limit,
                                    @RequestParam(defaultValue = "0") int offset) {

        JobQuery query = new JobQuery(tenantId, queue, state, jobType, limit, offset);
        return Map.of(
                "total", store.countJobs(query),
                "limit", query.limit(),
                "offset", query.offset(),
                "jobs", store.findJobs(query).stream().map(JobView::of).toList());
    }

    /**
     * Put a terminal job back on the queue.
     *
     * <p>409 rather than 400 when the job is not in a replayable state, because the request is
     * perfectly well formed — it just conflicts with where the job currently is. An operator who
     * clicks replay on a job that a worker picked up half a second ago should be told that, not
     * handed a validation error.
     */
    @PostMapping("/jobs/{id}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable UUID id) {
        if (store.find(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (!store.replay(id)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "job is not in a replayable state",
                    "state", store.find(id).map(job -> job.state().discriminator()).orElse("UNKNOWN")));
        }
        return ResponseEntity.ok(Map.of("id", id, "replayed", true));
    }

    @PostMapping("/jobs/{id}/cancel")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable UUID id,
                                                      @RequestParam(required = false) String reason) {
        if (store.find(id).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (!store.cancel(id, reason)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "only a pending job can be cancelled; in-flight work must stop cooperatively",
                    "state", store.find(id).map(job -> job.state().discriminator()).orElse("UNKNOWN")));
        }
        return ResponseEntity.ok(Map.of("id", id, "cancelled", true));
    }

    /**
     * Replay a batch of dead letters — the "the vendor is back up, push it all through again"
     * button. Bounded by a page size on purpose: replaying 200,000 dead letters in one click is how
     * an operator turns a resolved incident into a fresh one.
     */
    @PostMapping("/dlq/replay")
    public Map<String, Object> replayDeadLetters(@RequestParam(required = false) String tenantId,
                                                 @RequestParam(required = false) String queue,
                                                 @RequestParam(defaultValue = "100") int limit) {

        List<Job> deadLetters = store.findJobs(
                new JobQuery(tenantId, queue, "DEAD_LETTERED", null, limit, 0));

        long replayed = deadLetters.stream().filter(job -> store.replay(job.id())).count();

        return Map.of(
                "candidates", deadLetters.size(),
                "replayed", replayed,
                "remaining", store.countJobs(new JobQuery(tenantId, queue, "DEAD_LETTERED", null, 1, 0)));
    }

    @GetMapping("/outbox/stats")
    public Map<String, Long> outboxStats() {
        return Map.of(
                "pending", outbox.countInState("PENDING"),
                "published", outbox.countInState("PUBLISHED"),
                "deadLettered", outbox.countInState("DEAD_LETTERED"));
    }

    @GetMapping("/workers")
    public List<Map<String, Object>> workerStatus() {
        return workers.workers().stream().map(worker -> {
            WorkerMetrics.Snapshot snapshot = worker.metrics();
            return Map.<String, Object>of(
                    "workerId", worker.workerId(),
                    "inFlight", worker.inFlightCount(),
                    "metrics", snapshot);
        }).toList();
    }
}
