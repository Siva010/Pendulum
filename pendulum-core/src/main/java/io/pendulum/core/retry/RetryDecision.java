package io.pendulum.core.retry;

import java.time.Duration;

/** What the engine should do with a job that just threw. */
public sealed interface RetryDecision {

    /** Put it back on the queue, eligible again after {@code delay} measured from database now(). */
    record Retry(Duration delay, ErrorClass errorClass) implements RetryDecision {}

    /** Stop. Attempts exhausted, or the error class says retrying is pointless. */
    record DeadLetter(String reason, ErrorClass errorClass) implements RetryDecision {}
}
