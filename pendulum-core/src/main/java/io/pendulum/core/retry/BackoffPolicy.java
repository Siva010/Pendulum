package io.pendulum.core.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait before attempt {@code n}.
 *
 * <p>A function, not a {@code RetryStrategyFactory} hierarchy. There is exactly one
 * operation here and no state worth encapsulating, so a functional interface with static
 * factories carries the whole design with none of the ceremony.
 */
@FunctionalInterface
public interface BackoffPolicy {

    /**
     * @param attempt the attempt that just failed, 1-based
     * @return how long to wait before the next attempt
     */
    Duration delayBefore(int attempt);

    static BackoffPolicy fixed(Duration delay) {
        return attempt -> delay;
    }

    /** Deterministic exponential backoff. Useful in tests; in production, prefer jitter. */
    static BackoffPolicy exponential(Duration base, Duration cap) {
        return attempt -> capped(base, cap, attempt);
    }

    /**
     * Exponential backoff with <em>full</em> jitter: a uniform draw from
     * {@code [0, min(cap, base * 2^(attempt-1))]}.
     *
     * <p>Full jitter rather than "exponential plus a little noise" because the failure mode
     * being defended against is correlated retries: when a vendor has a 30-second outage,
     * every job that failed during it wakes at the same moment and re-creates the outage.
     * Spreading uniformly across the whole window is what actually decorrelates them.
     */
    static BackoffPolicy exponentialWithFullJitter(Duration base, Duration cap) {
        return attempt -> {
            long ceilingMillis = capped(base, cap, attempt).toMillis();
            if (ceilingMillis <= 0) return Duration.ZERO;
            return Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceilingMillis + 1));
        };
    }

    private static Duration capped(Duration base, Duration cap, int attempt) {
        int exponent = Math.max(0, attempt - 1);
        if (exponent >= 62) return cap;                       // 2^62 ms overflows anything sane
        long scaled;
        try {
            scaled = Math.multiplyExact(base.toMillis(), 1L << exponent);
        } catch (ArithmeticException overflow) {
            return cap;
        }
        return scaled >= cap.toMillis() ? cap : Duration.ofMillis(scaled);
    }
}
