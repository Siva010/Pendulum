package io.pendulum.core.retry;

import io.pendulum.core.domain.Job;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Error classification plus a backoff curve per class.
 *
 * <p>{@code Map<ErrorClass, BackoffPolicy>} rather than a strategy hierarchy: the variation
 * here is entirely in behaviour, and behaviour is what a function already is. A class per
 * strategy would add six files and relieve no force.
 */
public record RetryPolicy(ErrorClassifier classifier, Map<ErrorClass, BackoffPolicy> backoffs) {

    public RetryPolicy {
        if (classifier == null) throw new IllegalArgumentException("classifier is required");
        backoffs = Map.copyOf(backoffs);
        for (ErrorClass errorClass : ErrorClass.values()) {
            if (errorClass != ErrorClass.TERMINAL && !backoffs.containsKey(errorClass)) {
                throw new IllegalArgumentException("no backoff configured for " + errorClass);
            }
        }
    }

    /**
     * Sensible production defaults: one second doubling to five minutes for transient
     * failures, thirty seconds doubling to fifteen minutes when we have been rate limited.
     */
    public static RetryPolicy defaults() {
        Map<ErrorClass, BackoffPolicy> backoffs = new EnumMap<>(ErrorClass.class);
        backoffs.put(ErrorClass.TRANSIENT,
                BackoffPolicy.exponentialWithFullJitter(Duration.ofSeconds(1), Duration.ofMinutes(5)));
        backoffs.put(ErrorClass.RATE_LIMITED,
                BackoffPolicy.exponentialWithFullJitter(Duration.ofSeconds(30), Duration.ofMinutes(15)));
        backoffs.put(ErrorClass.TERMINAL, BackoffPolicy.fixed(Duration.ZERO));
        return new RetryPolicy(ErrorClassifier.defaults(), backoffs);
    }

    /**
     * Decide the fate of a failed attempt.
     *
     * <p>Note the attempt budget is read from the job row, not from this policy: attempts
     * are counted at claim time by the database, which is the only counter that survives a
     * worker being SIGKILLed mid-execution.
     */
    public RetryDecision decide(Job job, Throwable error) {
        ErrorClass errorClass = classifier.classify(error);
        if (errorClass == ErrorClass.TERMINAL) {
            return new RetryDecision.DeadLetter("terminal error: " + describe(error), errorClass);
        }
        if (job.isFinalAttempt()) {
            return new RetryDecision.DeadLetter(
                    "attempts exhausted (" + job.attempt() + "/" + job.maxAttempts() + "): " + describe(error),
                    errorClass);
        }
        Duration delay = backoffs.get(errorClass).delayBefore(job.attempt());
        return new RetryDecision.Retry(delay, errorClass);
    }

    private static String describe(Throwable error) {
        String message = error.getMessage();
        return error.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
