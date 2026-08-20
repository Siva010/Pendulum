package io.pendulum.core.engine;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.LeaseToken;
import io.pendulum.core.retry.RetryDecision;
import io.pendulum.core.retry.RetryPolicy;
import io.pendulum.core.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One worker: a poll loop, a bounded set of in-flight executions, and a heartbeat.
 *
 * <h2>Threading</h2>
 * The poll loop runs on a single platform thread — it is a tight loop around one blocking JDBC
 * call and gains nothing from being virtual. Job execution runs on virtual threads, which is the
 * whole reason this design is viable in 2024: job handlers are almost entirely I/O-bound (an HTTP
 * call, a database write, an S3 upload), so thread-per-job costs a few hundred bytes of heap
 * instead of a megabyte of stack, and blocking code stays blocking code.
 *
 * <p>Concurrency is bounded by a {@link Semaphore} rather than by a fixed pool size. A virtual
 * thread executor is unbounded by construction, so the bound has to come from somewhere, and a
 * permit per in-flight job is both the backpressure valve and the prefetch limit.
 *
 * <h2>Why a handler's failure never corrupts the queue</h2>
 * Every write reporting an execution outcome is conditional on the fencing token issued at claim
 * time. If this worker stalls long enough for its lease to expire and the job is reassigned, the
 * conditional write matches zero rows and the result is discarded rather than clobbering the new
 * owner's work. Timeouts alone cannot achieve this: there is no lease duration short enough to
 * out-run a stop-the-world pause and long enough to be useful.
 */
public final class Worker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final JobStore store;
    private final HandlerRegistry handlers;
    private final RetryPolicy retryPolicy;
    private final WorkerConfig config;
    private final WorkerMetrics metrics = new WorkerMetrics();

    private final Semaphore capacity;
    private final Map<UUID, Execution> inFlight = new ConcurrentHashMap<>();
    private final ExecutorService executionThreads =
            Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pendulum-heartbeat");
                thread.setDaemon(true);
                return thread;
            });

    private final AtomicBoolean running = new AtomicBoolean(false);
    /**
     * Set by {@link #terminateAbruptly()}. A crashed process writes nothing — no completion, no
     * failure, no lease release — so the flag suppresses every store write from threads that are
     * still unwinding. Without it, an in-process "crash" is not a crash but an orderly failure,
     * and the chaos suite would be testing the wrong thing.
     */
    private volatile boolean crashed;
    private volatile Thread pollThread;
    private final CountDownLatch stopped = new CountDownLatch(1);

    public Worker(JobStore store, HandlerRegistry handlers, RetryPolicy retryPolicy, WorkerConfig config) {
        this.store = store;
        this.handlers = handlers;
        this.retryPolicy = retryPolicy;
        this.config = config;
        this.capacity = new Semaphore(config.maxConcurrency());
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("worker " + config.workerId() + " is already running");
        }
        pollThread = new Thread(this::pollLoop, "pendulum-poll-" + config.queue());
        pollThread.start();
        long heartbeatMillis = config.heartbeatInterval().toMillis();
        heartbeats.scheduleAtFixedRate(
                this::renewLeases, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
        log.info("worker {} started on queue '{}' (concurrency={}, lease={})",
                config.workerId(), config.queue(), config.maxConcurrency(), config.leaseDuration());
    }

    public WorkerMetrics.Snapshot metrics() {
        return metrics.snapshot();
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    public String workerId() {
        return config.workerId();
    }

    // ---------------------------------------------------------------- polling

    private void pollLoop() {
        long idleMillis = config.minPollInterval().toMillis();

        while (running.get()) {
            try {
                int batch = Math.min(config.batchSize(), capacity.availablePermits());
                if (batch == 0) {
                    // Fully saturated. Sleeping beats spinning, and beats claiming work we cannot
                    // start — a claimed job sitting behind a busy worker is a job nobody is running.
                    sleep(config.minPollInterval().toMillis());
                    continue;
                }

                capacity.acquire(batch);
                List<Job> claimed;
                try {
                    claimed = store.claim(config.queue(), config.workerId(), batch, config.leaseDuration());
                } catch (RuntimeException e) {
                    capacity.release(batch);
                    log.warn("claim failed on queue '{}': {}", config.queue(), e.toString());
                    sleep(jitter(config.maxPollInterval().toMillis()));
                    continue;
                }
                capacity.release(batch - claimed.size());

                if (claimed.isEmpty()) {
                    metrics.recordEmptyPoll();
                    sleep(jitter(idleMillis));
                    // Adaptive backoff: an idle queue should not be polled at the same rate as a
                    // busy one. Twenty workers each polling every 50ms is 400 dispatch-index scans
                    // per second buying nothing, and the wasted scans are what turn into lock waits
                    // the moment work does arrive.
                    idleMillis = Math.min(idleMillis * 2, config.maxPollInterval().toMillis());
                    continue;
                }

                metrics.recordClaimed(claimed.size());
                idleMillis = config.minPollInterval().toMillis();
                for (Job job : claimed) {
                    dispatch(job);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                log.error("unexpected error in poll loop for queue '{}'", config.queue(), e);
                sleep(config.maxPollInterval().toMillis());
            }
        }
        stopped.countDown();
    }

    private void dispatch(Job job) {
        LeaseToken token = job.leaseToken().orElseThrow(
                () -> new IllegalStateException("claimed job " + job.id() + " carries no lease token"));
        Execution execution = new Execution(job, token);
        inFlight.put(job.id(), execution);
        try {
            executionThreads.execute(() -> execute(execution));
        } catch (RuntimeException e) {
            // The executor refused the task (shutdown raced with the poll loop). The job was never
            // started, so hand it straight back rather than making the queue wait out the lease.
            // Deliberately not rethrown: the rest of this batch still holds permits, and unwinding
            // out of the poll loop would strand them.
            inFlight.remove(job.id());
            capacity.release();
            log.warn("could not dispatch job {}; releasing the lease: {}", job.id(), e.toString());
            store.release(job.id(), token);
        }
    }

    // -------------------------------------------------------------- execution

    private void execute(Execution execution) {
        Job job = execution.job;
        LeaseToken token = execution.token;
        execution.runner = Thread.currentThread();
        try {
            if (!store.markRunning(job.id(), token)) {
                // Lost the lease between claim and start. Someone else owns it now; say nothing.
                execution.leaseHeld.set(false);
                metrics.recordLeaseLost();
                log.warn("lease lost before start for job {} ({})", job.id(), token);
                return;
            }
            execution.started = true;
            metrics.recordStarted();

            JobHandler handler = handlers.lookup(job.jobType()).orElse(null);
            if (handler == null) {
                // Not a poison pill — far more often a deploy in progress, where this worker has
                // not yet been updated with the new job type. Retry rather than dead-letter, and
                // let the ordinary attempt budget catch a type that genuinely never arrives.
                handleFailure(execution, new UnknownJobTypeException(job.jobType(), config.workerId()));
                return;
            }

            handler.handle(new JobContext(job, token, execution.leaseHeld::get));

            if (crashed) {
                return;
            }
            if (store.complete(job.id(), token)) {
                metrics.recordSucceeded();
            } else {
                // The work happened, but we no longer own the job — the fence rejected the write.
                // This is the lease race the whole design exists to survive: the new owner will run
                // it again, which is exactly why handlers must be idempotent.
                metrics.recordFencedWrite();
                log.warn("completion fenced off for job {} ({}): lease was reassigned mid-execution",
                        job.id(), token);
            }
        } catch (LeaseLostException e) {
            execution.leaseHeld.set(false);
            metrics.recordLeaseLost();
            log.warn("handler aborted for job {}: {}", job.id(), e.getMessage());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            handleFailure(execution, t);
        } finally {
            inFlight.remove(job.id());
            capacity.release();
        }
    }

    private void handleFailure(Execution execution, Throwable error) {
        Job job = execution.job;
        LeaseToken token = execution.token;

        if (crashed) {
            return;
        }
        if (!execution.leaseHeld.get()) {
            log.warn("suppressing failure record for job {}: lease already lost", job.id());
            return;
        }

        RetryDecision decision = retryPolicy.decide(job, error);
        boolean written = switch (decision) {
            case RetryDecision.Retry retry -> {
                log.info("job {} attempt {}/{} failed ({}), retrying in {}",
                        job.id(), job.attempt(), job.maxAttempts(), retry.errorClass(), retry.delay());
                metrics.recordRetried();
                yield store.retryLater(job.id(), token, retry.delay(), summarize(error));
            }
            case RetryDecision.DeadLetter deadLetter -> {
                log.warn("job {} dead-lettered after attempt {}/{}: {}",
                        job.id(), job.attempt(), job.maxAttempts(), deadLetter.reason());
                metrics.recordDeadLettered();
                yield store.deadLetter(job.id(), token, deadLetter.reason() + " | " + summarize(error));
            }
        };

        if (!written) {
            metrics.recordFencedWrite();
            log.warn("failure record fenced off for job {} ({}): lease was reassigned", job.id(), token);
        }
    }

    // ------------------------------------------------------------- heartbeats

    /**
     * Renew every in-flight lease. A worker that is alive but slow keeps its jobs; a worker that
     * is gone stops renewing and the reaper takes them back after the lease duration.
     *
     * <p>A failed renewal means the lease is already gone, so the execution is marked lost and
     * interrupted. Interruption is best-effort — a handler blocked in a socket read with no
     * timeout will not notice — which is why the write-path fence, not the interrupt, is what
     * actually guarantees correctness.
     */
    private void renewLeases() {
        for (Execution execution : inFlight.values()) {
            if (!execution.started || !execution.leaseHeld.get()) {
                continue;
            }
            try {
                if (!store.heartbeat(execution.job.id(), execution.token, config.leaseDuration())) {
                    execution.leaseHeld.set(false);
                    metrics.recordLeaseLost();
                    log.warn("lost lease for in-flight job {} ({}); interrupting handler",
                            execution.job.id(), execution.token);
                    Thread runner = execution.runner;
                    if (runner != null) {
                        runner.interrupt();
                    }
                }
            } catch (RuntimeException e) {
                // A failed heartbeat round-trip is not proof the lease is gone — the next beat may
                // succeed. Losing a lease on a transient database blip would be a self-inflicted
                // duplicate execution, so we log and let the lease duration be the real deadline.
                log.warn("heartbeat failed for job {}: {}", execution.job.id(), e.toString());
            }
        }
    }

    // --------------------------------------------------------------- shutdown

    /**
     * Graceful shutdown, the SIGTERM path.
     *
     * <p>Stop claiming, let in-flight work finish inside the drain deadline, and explicitly hand
     * back anything that never started. The explicit release is what makes a rolling deploy cheap:
     * without it, every job a terminating pod had claimed waits out the full lease duration before
     * anyone else can touch it.
     *
     * <p>Work still running when the deadline passes is deliberately <em>not</em> released. We
     * cannot know whether its side effects happened, so it is left to expire and be reaped — the
     * at-least-once path, which handlers are already required to tolerate.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("worker {} draining (timeout={})", config.workerId(), config.drainTimeout());
        Thread poller = pollThread;
        if (poller != null) {
            poller.interrupt();
        }
        awaitQuietly(stopped, config.drainTimeout());

        long deadline = System.nanoTime() + config.drainTimeout().toNanos();
        while (!inFlight.isEmpty() && System.nanoTime() < deadline) {
            sleep(25);
        }

        heartbeats.shutdownNow();

        int released = 0;
        int abandoned = 0;
        for (Execution execution : inFlight.values()) {
            if (!execution.started && store.release(execution.job.id(), execution.token)) {
                released++;
            } else {
                abandoned++;
            }
        }
        executionThreads.shutdownNow();

        log.info("worker {} stopped: {} released, {} left to lease expiry, metrics={}",
                config.workerId(), released, abandoned, metrics.snapshot());
    }

    /**
     * Abrupt stop with no drain and no lease release — what a {@code kill -9} would do, minus the
     * dead JVM. Used by the chaos suite to prove that recovery does not depend on cooperative
     * shutdown.
     */
    public void terminateAbruptly() {
        crashed = true;
        running.set(false);
        Thread poller = pollThread;
        if (poller != null) {
            poller.interrupt();
        }
        heartbeats.shutdownNow();
        executionThreads.shutdownNow();
        WorkerMetrics.Snapshot snapshot = metrics.snapshot();
        inFlight.clear();
        log.warn("worker {} terminated abruptly; {} leases left to expire",
                config.workerId(), snapshot.started() - snapshot.settled());
    }

    // ---------------------------------------------------------------- helpers

    /** One in-flight job and the mutable bits the heartbeat thread needs to see. */
    private static final class Execution {
        private final Job job;
        private final LeaseToken token;
        private final AtomicBoolean leaseHeld = new AtomicBoolean(true);
        private volatile boolean started;
        private volatile Thread runner;

        private Execution(Job job, LeaseToken token) {
            this.job = job;
            this.token = token;
        }
    }

    private static String summarize(Throwable error) {
        StringBuilder chain = new StringBuilder();
        Throwable cause = error;
        int depth = 0;
        while (cause != null && depth++ < 5) {
            if (!chain.isEmpty()) chain.append(" <- ");
            chain.append(cause.getClass().getSimpleName());
            if (cause.getMessage() != null) chain.append(": ").append(cause.getMessage());
            cause = cause.getCause();
        }
        return chain.toString();
    }

    /** ±25% so a fleet of workers does not synchronise into a thundering herd. */
    private static long jitter(long millis) {
        long spread = Math.max(1, millis / 4);
        return millis + ThreadLocalRandom.current().nextLong(-spread, spread + 1);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(Math.max(1, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitQuietly(CountDownLatch latch, Duration timeout) {
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class UnknownJobTypeException extends RuntimeException {
        private UnknownJobTypeException(String jobType, String workerId) {
            super("no handler registered for job type '" + jobType + "' on worker " + workerId);
        }
    }
}
