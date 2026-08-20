package io.pendulum.core;

import io.pendulum.core.domain.NewJob;
import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.engine.JobHandler;
import io.pendulum.core.engine.LeaseReaper;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.store.JobStore;
import io.pendulum.core.store.PostgresJobStore;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The embedding surface: build one of these, register handlers, start it.
 *
 * <pre>{@code
 * Pendulum pendulum = Pendulum.builder(dataSource)
 *         .migrate()
 *         .handler("send-welcome-email", ctx -> mailer.send(ctx.payload()))
 *         .workers(2)
 *         .build();
 * pendulum.start();
 * pendulum.enqueue(NewJob.of("acme", "send-welcome-email").payload(json).build());
 * }</pre>
 *
 * <p>Note what is absent: any Spring type. The engine runs from a bare {@link DataSource}, which
 * is what makes it testable without an application context and embeddable in something that is
 * not a Spring app. {@code pendulum-server} wires this into Boot at the edge; the engine does not
 * know that Boot exists.
 */
public final class Pendulum implements AutoCloseable {

    private final JobStore store;
    private final List<Worker> workers;
    private final LeaseReaper reaper;

    private Pendulum(JobStore store, List<Worker> workers, LeaseReaper reaper) {
        this.store = store;
        this.workers = List.copyOf(workers);
        this.reaper = reaper;
    }

    public static Builder builder(DataSource dataSource) {
        return new Builder(dataSource);
    }

    /** Apply the Pendulum schema migrations to {@code dataSource}. */
    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .table("pendulum_schema_history")
                .load()
                .migrate();
    }

    public UUID enqueue(NewJob job) {
        return store.enqueue(job).id();
    }

    public JobStore store() {
        return store;
    }

    public List<Worker> workers() {
        return workers;
    }

    public LeaseReaper reaper() {
        return reaper;
    }

    public void start() {
        reaper.start();
        workers.forEach(Worker::start);
    }

    @Override
    public void close() {
        // Workers first: draining releases leases, and a reaper still running while they drain
        // keeps recovery honest if a drain overruns.
        workers.forEach(Worker::close);
        reaper.close();
    }

    public static final class Builder {

        private final DataSource dataSource;
        private final HandlerRegistry handlers = new HandlerRegistry();
        private RetryPolicy retryPolicy = RetryPolicy.defaults();
        private WorkerConfig workerConfig = WorkerConfig.defaults(NewJob.DEFAULT_QUEUE);
        private int workerCount = 1;
        private boolean migrate;

        private Builder(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Builder migrate() {
            this.migrate = true;
            return this;
        }

        public Builder handler(String jobType, JobHandler handler) {
            handlers.register(jobType, handler);
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder workerConfig(WorkerConfig workerConfig) {
            this.workerConfig = workerConfig;
            return this;
        }

        /** In-process workers. Real horizontal scale comes from more processes, not more of these. */
        public Builder workers(int workerCount) {
            this.workerCount = workerCount;
            return this;
        }

        public Pendulum build() {
            if (migrate) {
                Pendulum.migrate(dataSource);
            }
            JobStore store = new PostgresJobStore(dataSource);
            List<Worker> workers = new ArrayList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                WorkerConfig config = workerCount == 1
                        ? workerConfig
                        : workerConfig.withWorkerId(workerConfig.workerId() + "#" + i);
                workers.add(new Worker(store, handlers, retryPolicy, config));
            }
            return new Pendulum(store, workers, LeaseReaper.defaults(store));
        }
    }
}
