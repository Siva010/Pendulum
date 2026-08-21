package io.pendulum.server;

import io.pendulum.core.json.JsonPayloads;
import io.pendulum.core.workflow.WorkflowEngine;
import io.pendulum.core.workflow.WorkflowRun;
import io.pendulum.core.workflow.WorkflowStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Workflow runs: start one, inspect its step history. */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;
    private final WorkflowStore runs;

    public WorkflowController(WorkflowEngine engine, WorkflowStore runs) {
        this.engine = engine;
        this.runs = runs;
    }

    public record StartRequest(String tenantId, Object input) {}

    public record RunView(
            UUID id,
            String tenantId,
            String workflowType,
            String state,
            int completedSteps,
            UUID jobId,
            String result,
            String lastError,
            Instant createdAt,
            Instant completedAt
    ) {
        static RunView of(WorkflowRun run) {
            return new RunView(run.id(), run.tenantId(), run.workflowType(), run.state(),
                    run.completedSteps(), run.jobId(), run.result(), run.lastError(),
                    run.createdAt(), run.completedAt());
        }
    }

    @PostMapping("/{type}/runs")
    public ResponseEntity<?> start(@PathVariable String type, @RequestBody StartRequest request) {
        try {
            UUID runId = engine.start(request.tenantId(), type,
                    request.input() == null ? "{}" : JsonPayloads.toJson(request.input()));
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("runId", runId, "workflowType", type));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/runs")
    public List<RunView> list(@RequestParam(required = false) String tenantId,
                              @RequestParam(defaultValue = "50") int limit) {
        return runs.recentRuns(tenantId, limit).stream().map(RunView::of).toList();
    }

    /** A run plus its committed step history — what actually ran, and what each step produced. */
    @GetMapping("/runs/{id}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID id) {
        return runs.find(id)
                .map(run -> ResponseEntity.ok(Map.<String, Object>of(
                        "run", RunView.of(run),
                        "steps", runs.stepResults(id).values())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
