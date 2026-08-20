package io.pendulum.core.retry;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.JobState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure unit tests — no database, no container. These are the fast feedback loop. */
class RetryPolicyTest {

    @Test
    @DisplayName("exponential backoff doubles and then stops at the cap")
    void exponential_backoff_is_capped() {
        BackoffPolicy policy = BackoffPolicy.exponential(Duration.ofSeconds(1), Duration.ofSeconds(30));

        assertThat(policy.delayBefore(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayBefore(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayBefore(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(policy.delayBefore(6)).isEqualTo(Duration.ofSeconds(30));
        assertThat(policy.delayBefore(60)).as("no overflow").isEqualTo(Duration.ofSeconds(30));
    }

    /**
     * Full jitter draws uniformly from {@code [0, ceiling]} rather than adding noise to a fixed
     * value. The point is decorrelation: after a vendor outage, every job that failed during it
     * must not wake at the same instant and re-create the outage.
     */
    @Test
    @DisplayName("full jitter stays within the exponential ceiling and actually varies")
    void full_jitter_spreads_within_the_ceiling() {
        BackoffPolicy policy =
                BackoffPolicy.exponentialWithFullJitter(Duration.ofSeconds(1), Duration.ofMinutes(5));

        Duration ceiling = Duration.ofSeconds(8); // 1s * 2^3 for attempt 4
        boolean sawVariation = false;
        Duration first = policy.delayBefore(4);

        for (int i = 0; i < 200; i++) {
            Duration delay = policy.delayBefore(4);
            assertThat(delay).isBetween(Duration.ZERO, ceiling);
            sawVariation |= !delay.equals(first);
        }

        assertThat(sawVariation).as("jitter that never varies is not jitter").isTrue();
    }

    @Test
    @DisplayName("a marked terminal error is never retried, however many attempts remain")
    void terminal_errors_are_not_retried() {
        RetryPolicy policy = RetryPolicy.defaults();
        Job job = jobOnAttempt(1, 10);

        RetryDecision decision = policy.decide(job, new TerminalJobException("422 unprocessable"));

        assertThat(decision).isInstanceOf(RetryDecision.DeadLetter.class);
        assertThat(((RetryDecision.DeadLetter) decision).errorClass()).isEqualTo(ErrorClass.TERMINAL);
    }

    @Test
    @DisplayName("a transient error retries while the budget lasts")
    void transient_errors_are_retried() {
        RetryPolicy policy = RetryPolicy.defaults();

        RetryDecision decision = policy.decide(jobOnAttempt(2, 5), new SocketTimeoutException("read timeout"));

        assertThat(decision).isInstanceOf(RetryDecision.Retry.class);
        assertThat(((RetryDecision.Retry) decision).errorClass()).isEqualTo(ErrorClass.TRANSIENT);
    }

    @Test
    @DisplayName("the last attempt dead-letters rather than retrying into nothing")
    void the_final_attempt_dead_letters() {
        RetryPolicy policy = RetryPolicy.defaults();

        RetryDecision decision = policy.decide(jobOnAttempt(5, 5), new IllegalStateException("503"));

        assertThat(decision).isInstanceOf(RetryDecision.DeadLetter.class);
        assertThat(((RetryDecision.DeadLetter) decision).reason()).contains("attempts exhausted (5/5)");
    }

    @Test
    @DisplayName("rate limiting backs off far harder than an ordinary blip")
    void rate_limiting_uses_its_own_curve() {
        RetryPolicy policy = RetryPolicy.defaults();
        Job job = jobOnAttempt(1, 5);

        Duration longestBlip = Duration.ZERO;
        Duration longestThrottle = Duration.ZERO;
        for (int i = 0; i < 50; i++) {
            longestBlip = max(longestBlip,
                    ((RetryDecision.Retry) policy.decide(job, new IllegalStateException("503"))).delay());
            longestThrottle = max(longestThrottle,
                    ((RetryDecision.Retry) policy.decide(job, new RateLimitedException("429"))).delay());
        }

        assertThat(longestBlip).as("a 503 waits at most a second on the first attempt")
                .isLessThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(longestThrottle).as("a 429 backs off on a much longer curve")
                .isGreaterThan(Duration.ofSeconds(1))
                .isLessThanOrEqualTo(Duration.ofSeconds(30));
    }

    /**
     * Classification has to see through wrapping. Handlers routinely catch and rewrap, and a
     * classifier that only inspected the outermost type would treat a wrapped
     * {@code TerminalJobException} as transient and retry a request the server will always reject.
     */
    @Test
    @DisplayName("classification unwraps the cause chain")
    void classification_looks_at_the_whole_cause_chain() {
        ErrorClassifier classifier = ErrorClassifier.defaults();

        Throwable wrapped = new RuntimeException("handler failed",
                new IOException("upstream", new TerminalJobException("422")));

        assertThat(classifier.classify(wrapped)).isEqualTo(ErrorClass.TERMINAL);
        assertThat(classifier.classify(new RuntimeException("who knows"))).isEqualTo(ErrorClass.TRANSIENT);
    }

    @Test
    @DisplayName("a cyclic cause chain does not hang the classifier")
    void classification_survives_a_cause_cycle() {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        first.initCause(second);
        second.initCause(first);

        assertThat(ErrorClassifier.defaults().classify(first)).isEqualTo(ErrorClass.TRANSIENT);
    }

    private static Duration max(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static Job jobOnAttempt(int attempt, int maxAttempts) {
        return new Job(UUID.randomUUID(), "tenant-a", "default", "test", "{}",
                new JobState.Running(new io.pendulum.core.domain.LeaseToken(1), "worker-a",
                        Instant.now().plusSeconds(30)),
                0, Instant.now(), attempt, maxAttempts, null, null, Instant.now(), Instant.now());
    }
}
