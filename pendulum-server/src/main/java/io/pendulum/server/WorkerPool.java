package io.pendulum.server;

import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerMetrics;

import java.util.List;

/**
 * The workers running in this process, as one bean.
 *
 * <p>A named type rather than a {@code List<Worker>} bean: Spring resolves an injected
 * {@code List<Worker>} by collecting beans <em>of type Worker</em>, and only falls back to a bean
 * that happens to be a list. That fallback works until someone defines a single {@code Worker}
 * bean elsewhere and the list silently becomes something else.
 */
public record WorkerPool(List<Worker> workers) implements AutoCloseable {

    public WorkerPool {
        workers = List.copyOf(workers);
    }

    public int size() {
        return workers.size();
    }

    public void start() {
        workers.forEach(Worker::start);
    }

    /** Aggregate counters across the pool — what the admin API and Actuator report. */
    public WorkerMetrics.Snapshot aggregateMetrics() {
        return workers.stream()
                .map(Worker::metrics)
                .reduce(new WorkerMetrics.Snapshot(0, 0, 0, 0, 0, 0, 0, 0), WorkerPool::sum);
    }

    public int inFlight() {
        return workers.stream().mapToInt(Worker::inFlightCount).sum();
    }

    @Override
    public void close() {
        workers.forEach(Worker::close);
    }

    private static WorkerMetrics.Snapshot sum(WorkerMetrics.Snapshot a, WorkerMetrics.Snapshot b) {
        return new WorkerMetrics.Snapshot(
                a.claimed() + b.claimed(),
                a.started() + b.started(),
                a.succeeded() + b.succeeded(),
                a.retried() + b.retried(),
                a.deadLettered() + b.deadLettered(),
                a.leasesLost() + b.leasesLost(),
                a.fencedWrites() + b.fencedWrites(),
                a.emptyPolls() + b.emptyPolls());
    }
}
