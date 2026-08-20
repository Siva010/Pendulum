package io.pendulum.server;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.NewJob;
import io.pendulum.core.domain.Schedule;
import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerMetrics;
import io.pendulum.core.store.JobStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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

    public JobsController(JobStore store, WorkerPool workers) {
        this.store = store;
        this.workers = workers;
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
                    job.runAt(), job.lastError(), owner, expiry, job.createdAt(), job.updatedAt());
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
