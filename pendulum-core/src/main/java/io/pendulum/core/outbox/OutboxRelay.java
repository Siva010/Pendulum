package io.pendulum.core.outbox;

import io.pendulum.core.retry.BackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Drains the outbox, one batch at a time.
 *
 * <h2>Why the publish happens outside any transaction</h2>
 * The tempting implementation holds {@code SELECT ... FOR UPDATE} open, publishes, then commits.
 * It is wrong twice over: it pins a connection and an MVCC snapshot for the duration of a network
 * call to a system you do not control, and it does not even buy exactly-once — a crash between the
 * publish and the commit still republishes. So the relay claims by pushing a visibility timeout
 * forward, publishes with nothing held, and records the outcome afterwards.
 *
 * <h2>What this guarantees</h2>
 * At-least-once delivery. A relay that publishes and dies before recording success will publish
 * again when the timeout lapses. That is unavoidable without a distributed transaction across
 * Postgres and the destination, which does not exist — hence {@code message_key}, so consumers can
 * make the replay harmless.
 *
 * <p>Safe to run on every node: the claim uses {@code SKIP LOCKED}, so relays partition the work
 * rather than fight over it.
 */
public final class OutboxRelay implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore store;
    private final OutboxPublisher publisher;
    private final Duration pollInterval;
    private final Duration visibilityTimeout;
    private final int batchSize;
    private final BackoffPolicy backoff;

    private final LongAdder published = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder deadLettered = new LongAdder();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pendulum-outbox-relay");
                thread.setDaemon(true);
                return thread;
            });

    public OutboxRelay(OutboxStore store,
                       OutboxPublisher publisher,
                       Duration pollInterval,
                       Duration visibilityTimeout,
                       int batchSize,
                       BackoffPolicy backoff) {
        if (visibilityTimeout.compareTo(pollInterval) <= 0) {
            // Otherwise a message becomes claimable again before the relay that holds it has had a
            // chance to publish, and every message is delivered twice by construction.
            throw new IllegalArgumentException(
                    "visibilityTimeout (" + visibilityTimeout + ") must exceed pollInterval (" + pollInterval + ")");
        }
        this.store = store;
        this.publisher = publisher;
        this.pollInterval = pollInterval;
        this.visibilityTimeout = visibilityTimeout;
        this.batchSize = batchSize;
        this.backoff = backoff;
    }

    public static OutboxRelay defaults(OutboxStore store, OutboxPublisher publisher) {
        return new OutboxRelay(store, publisher,
                Duration.ofMillis(500),
                Duration.ofSeconds(30),
                100,
                BackoffPolicy.exponentialWithFullJitter(Duration.ofSeconds(1), Duration.ofMinutes(10)));
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long millis = pollInterval.toMillis();
        long initialDelay = ThreadLocalRandom.current().nextLong(millis + 1);
        scheduler.scheduleWithFixedDelay(this::drainOnce, initialDelay, millis, TimeUnit.MILLISECONDS);
        log.info("outbox relay started (poll={}, visibility={}, batch={})",
                pollInterval, visibilityTimeout, batchSize);
    }

    /**
     * Publish one batch.
     *
     * @return how many were published successfully
     */
    public int drainOnce() {
        int publishedNow = 0;
        try {
            List<OutboxMessage> batch = store.claimForPublishing(batchSize, visibilityTimeout);
            for (OutboxMessage message : batch) {
                if (publish(message)) {
                    publishedNow++;
                }
            }
        } catch (RuntimeException e) {
            // scheduleWithFixedDelay cancels the task permanently if it throws, and a silently dead
            // relay means effects stop happening while the business writes keep committing — the
            // failure mode is invisible until someone asks why no emails went out.
            log.error("outbox drain failed", e);
        }
        return publishedNow;
    }

    private boolean publish(OutboxMessage message) {
        try {
            publisher.publish(message);
            if (store.markPublished(message.id())) {
                published.increment();
                return true;
            }
            // Another relay already recorded it. The effect happened at least once, which is the
            // contract; nothing to do but note it.
            log.debug("outbox message {} was already marked published", message.id());
            return false;
        } catch (Exception e) {
            recordFailure(message, e);
            return false;
        }
    }

    private void recordFailure(OutboxMessage message, Exception error) {
        String description = error.getClass().getSimpleName()
                + (error.getMessage() == null ? "" : ": " + error.getMessage());

        if (message.isFinalAttempt()) {
            log.warn("outbox message {} dead-lettered after {}/{} attempts: {}",
                    message.id(), message.attempts(), message.maxAttempts(), description);
            store.markDeadLettered(message.id(),
                    "attempts exhausted (" + message.attempts() + "/" + message.maxAttempts() + "): " + description);
            deadLettered.increment();
            return;
        }

        Duration delay = backoff.delayBefore(message.attempts());
        log.info("outbox message {} attempt {}/{} failed ({}), retrying in {}",
                message.id(), message.attempts(), message.maxAttempts(), description, delay);
        store.markFailed(message.id(), delay, description);
        failed.increment();
    }

    public long totalPublished() { return published.sum(); }

    public long totalFailed() { return failed.sum(); }

    public long totalDeadLettered() { return deadLettered.sum(); }

    @Override
    public void close() {
        running.set(false);
        scheduler.shutdownNow();
    }
}
