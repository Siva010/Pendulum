package io.pendulum.core.engine;

/**
 * Application code that does the actual work.
 *
 * <p>A functional interface with one method: there is nothing here worth a class hierarchy,
 * and keeping it a lambda target means application code registers a handler in one line
 * without implementing an interface it has to import into its domain.
 *
 * <p><strong>Handlers must be idempotent.</strong> Pendulum guarantees at-least-once
 * <em>delivery</em>; effectively-once <em>effects</em> come from the handler pairing an
 * idempotency key with its side effect. A handler that charges a card without one will
 * eventually charge twice, and no amount of leasing prevents it.
 */
@FunctionalInterface
public interface JobHandler {

    /**
     * @throws io.pendulum.core.retry.TerminalJobException to skip retries entirely
     * @throws io.pendulum.core.retry.RateLimitedException to retry on the slow backoff curve
     * @throws Exception any other exception is classified as transient and retried
     */
    void handle(JobContext context) throws Exception;
}
