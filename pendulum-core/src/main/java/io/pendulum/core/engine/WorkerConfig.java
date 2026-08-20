package io.pendulum.core.engine;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * How one worker behaves.
 *
 * <p>The constructor enforces the invariants that are easy to get wrong and expensive to
 * discover in production — chiefly that heartbeats must be frequent enough relative to the
 * lease that one slow beat cannot let the lease lapse under a healthy worker.
 *
 * @param batchSize          jobs claimed per poll. Larger amortises the round trip; too large
 *                           and one worker hoards work its peers could have started sooner.
 * @param maxConcurrency     bounded in-flight work. This is the backpressure valve: never pull
 *                           10,000 jobs into memory because the queue happens to be deep.
 * @param leaseDuration      how long a claim is valid without a heartbeat. This is also the
 *                           worst-case recovery time after a SIGKILL, so it trades recovery
 *                           latency against tolerance for stop-the-world pauses.
 * @param heartbeatInterval  how often in-flight leases are renewed.
 * @param minPollInterval    poll delay when the queue is busy.
 * @param maxPollInterval    poll delay after repeated empty polls — adaptive backoff, so twenty
 *                           idle workers are not hammering the dispatch index once a second.
 * @param drainTimeout       how long {@code close()} waits for in-flight work on SIGTERM.
 */
public record WorkerConfig(
        String workerId,
        String queue,
        int batchSize,
        int maxConcurrency,
        Duration leaseDuration,
        Duration heartbeatInterval,
        Duration minPollInterval,
        Duration maxPollInterval,
        Duration drainTimeout
) {

    public WorkerConfig {
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId is required");
        if (queue == null || queue.isBlank()) throw new IllegalArgumentException("queue is required");
        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");
        if (maxConcurrency <= 0) throw new IllegalArgumentException("maxConcurrency must be positive");
        if (batchSize > maxConcurrency) {
            throw new IllegalArgumentException(
                    "batchSize (" + batchSize + ") cannot exceed maxConcurrency (" + maxConcurrency + ")");
        }
        // Two missed beats must still leave room to renew. Without this, a worker that is
        // perfectly healthy but momentarily slow loses leases it is actively working on, and
        // the resulting duplicate executions look like a fencing bug when they are a config bug.
        if (heartbeatInterval.multipliedBy(3).compareTo(leaseDuration) > 0) {
            throw new IllegalArgumentException(
                    "heartbeatInterval (" + heartbeatInterval + ") must be at most a third of "
                    + "leaseDuration (" + leaseDuration + ")");
        }
        if (minPollInterval.compareTo(maxPollInterval) > 0) {
            throw new IllegalArgumentException("minPollInterval must not exceed maxPollInterval");
        }
    }

    public static WorkerConfig defaults(String queue) {
        return new WorkerConfig(
                generateWorkerId(),
                queue,
                /* batchSize */ 16,
                /* maxConcurrency */ 64,
                /* leaseDuration */ Duration.ofSeconds(30),
                /* heartbeatInterval */ Duration.ofSeconds(5),
                /* minPollInterval */ Duration.ofMillis(50),
                /* maxPollInterval */ Duration.ofSeconds(2),
                /* drainTimeout */ Duration.ofSeconds(20));
    }

    public WorkerConfig withWorkerId(String workerId) {
        return new WorkerConfig(workerId, queue, batchSize, maxConcurrency, leaseDuration,
                heartbeatInterval, minPollInterval, maxPollInterval, drainTimeout);
    }

    public WorkerConfig withLease(Duration leaseDuration, Duration heartbeatInterval) {
        return new WorkerConfig(workerId, queue, batchSize, maxConcurrency, leaseDuration,
                heartbeatInterval, minPollInterval, maxPollInterval, drainTimeout);
    }

    public WorkerConfig withConcurrency(int batchSize, int maxConcurrency) {
        return new WorkerConfig(workerId, queue, batchSize, maxConcurrency, leaseDuration,
                heartbeatInterval, minPollInterval, maxPollInterval, drainTimeout);
    }

    public WorkerConfig withPollInterval(Duration min, Duration max) {
        return new WorkerConfig(workerId, queue, batchSize, maxConcurrency, leaseDuration,
                heartbeatInterval, min, max, drainTimeout);
    }

    public WorkerConfig withDrainTimeout(Duration drainTimeout) {
        return new WorkerConfig(workerId, queue, batchSize, maxConcurrency, leaseDuration,
                heartbeatInterval, minPollInterval, maxPollInterval, drainTimeout);
    }

    /**
     * host/pid/random. The random suffix matters: two pods can share a hostname across a
     * restart, and a reused worker id makes the admin UI lie about who owns a lease.
     */
    private static String generateWorkerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        String random = Long.toHexString(ThreadLocalRandom.current().nextLong() >>> 40);
        return host + "/" + pid + "/" + random;
    }
}
