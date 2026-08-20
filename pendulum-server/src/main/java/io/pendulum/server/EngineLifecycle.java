package io.pendulum.server;

import io.pendulum.core.engine.LeaseReaper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Starts polling only once the context is fully refreshed, and drains on SIGTERM.
 *
 * <p>Starting inside a {@code @Bean} method would have workers pulling jobs while the rest of the
 * application is still being constructed, so a handler could run against a half-initialised
 * dependency. {@link SmartLifecycle} is the hook that guarantees "everything else is ready".
 *
 * <p>The drain must fit inside the pod's {@code terminationGracePeriodSeconds}. If it does not,
 * Kubernetes SIGKILLs the process mid-drain and every job it was holding waits out its full lease
 * before anyone else can touch it — a rolling deploy that looks like an outage.
 */
@Component
public class EngineLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EngineLifecycle.class);

    private final WorkerPool pool;
    private final LeaseReaper reaper;
    private volatile boolean running;

    public EngineLifecycle(WorkerPool pool, LeaseReaper reaper) {
        this.pool = pool;
        this.reaper = reaper;
    }

    @Override
    public void start() {
        reaper.start();
        pool.start();
        running = true;
        log.info("Pendulum engine started with {} worker(s)", pool.size());
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        log.info("draining Pendulum engine");
        pool.close();
        reaper.close();
        log.info("Pendulum engine stopped: {}", pool.aggregateMetrics());
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Stop before the web server does, so in-flight jobs finish while the process is otherwise
     * still healthy — and start after it, so the pod is only marked ready once it can serve.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }
}
