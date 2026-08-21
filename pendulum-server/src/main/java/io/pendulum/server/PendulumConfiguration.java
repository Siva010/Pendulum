package io.pendulum.server;

import io.pendulum.core.engine.HandlerRegistry;
import io.pendulum.core.outbox.OutboxPublisher;
import io.pendulum.core.outbox.OutboxRelay;
import io.pendulum.core.outbox.OutboxStore;
import io.pendulum.core.outbox.PostgresOutboxStore;
import io.pendulum.core.engine.LeaseReaper;
import io.pendulum.core.engine.Worker;
import io.pendulum.core.engine.WorkerConfig;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.retry.TerminalJobException;
import io.pendulum.core.store.JobStore;
import io.pendulum.core.store.PostgresJobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/**
 * The seam between Spring and the engine.
 *
 * <p>Everything Spring-shaped stops here: below this class it is a {@link DataSource} and plain
 * objects. Note in particular that there is no {@code @Transactional} anywhere in the engine —
 * the engine's unit of atomicity is a single SQL statement, and wrapping those in a proxy-managed
 * transaction would add a round trip and a held connection without adding any atomicity.
 */
@Configuration
public class PendulumConfiguration {

    @Bean
    public JobStore jobStore(DataSource dataSource) {
        return new PostgresJobStore(dataSource);
    }

    @Bean
    public RetryPolicy retryPolicy() {
        return RetryPolicy.defaults();
    }

    /**
     * Handlers registered out of the box, so a fresh clone has something to run. Replace these with
     * your own — or better, register them from the module that owns the work rather than here.
     */
    @Bean
    public HandlerRegistry handlerRegistry() {
        HandlerRegistry registry = new HandlerRegistry();

        registry.register("noop", context -> { });

        registry.register("sleep", context -> {
            // Demonstrates the heartbeat: this job outlives several beats and keeps its lease.
            Thread.sleep(java.time.Duration.ofSeconds(10));
        });

        registry.register("boom", context -> {
            throw new IllegalStateException("simulated transient failure (attempt "
                    + context.attempt() + ")");
        });

        registry.register("poison", context -> {
            throw new TerminalJobException("simulated 422: this will never succeed");
        });

        return registry;
    }

    @Bean
    public WorkerPool workerPool(JobStore store,
                                 HandlerRegistry handlers,
                                 RetryPolicy retryPolicy,
                                 PendulumProperties properties,
                                 @Value("${spring.application.name:pendulum}") String appName) {

        WorkerConfig base = WorkerConfig.defaults(properties.queue())
                .withConcurrency(properties.batchSize(), properties.maxConcurrency())
                .withLease(properties.leaseDuration(), properties.heartbeatInterval())
                .withPollInterval(properties.minPollInterval(), properties.maxPollInterval())
                .withDrainTimeout(properties.drainTimeout());

        List<Worker> workers = new ArrayList<>(properties.workers());
        for (int i = 0; i < properties.workers(); i++) {
            WorkerConfig config = base.withWorkerId(appName + "/" + base.workerId() + "#" + i);
            workers.add(new Worker(store, handlers, retryPolicy, config));
        }
        return new WorkerPool(workers);
    }

    @Bean
    public LeaseReaper leaseReaper(JobStore store, PendulumProperties properties) {
        return new LeaseReaper(store, properties.reapInterval(), properties.reapBatchSize());
    }

    @Bean
    public OutboxStore outboxStore(DataSource dataSource) {
        return new PostgresOutboxStore(dataSource);
    }

    /**
     * Replace this with the adapter for wherever your effects actually go — a Kafka producer, an
     * HTTP client, an SES client. It logs by default so a fresh clone demonstrates the flow without
     * requiring a broker, and so that forgetting to replace it is visible rather than silent.
     *
     * <p>Whatever you write here, propagate {@code messageKey} onto the outgoing message. Delivery
     * is at-least-once, and that key is how the consumer makes a replay harmless.
     */
    @Bean
    @ConditionalOnMissingBean(OutboxPublisher.class)
    public OutboxPublisher loggingOutboxPublisher() {
        Logger publisherLog = LoggerFactory.getLogger("io.pendulum.outbox.publisher");
        return message -> publisherLog.info(
                "PUBLISH destination={} key={} headers={} payload={}",
                message.destination(), message.messageKey(), message.headers(), message.payload());
    }

    @Bean
    public OutboxRelay outboxRelay(OutboxStore outboxStore, OutboxPublisher publisher) {
        return OutboxRelay.defaults(outboxStore, publisher);
    }

}
