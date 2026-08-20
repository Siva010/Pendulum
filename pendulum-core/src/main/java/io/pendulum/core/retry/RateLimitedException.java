package io.pendulum.core.retry;

/** Thrown by a handler when a downstream asked us to back off. */
public class RateLimitedException extends RuntimeException {

    public RateLimitedException(String message) {
        super(message);
    }

    public RateLimitedException(String message, Throwable cause) {
        super(message, cause);
    }
}
