package io.pendulum.core.retry;

/**
 * How a failure should be treated by the retry engine.
 *
 * <p>The distinction is the whole point of the classifier: retrying everything is how you
 * turn one bad request into fifty and DDoS your own vendor, and retrying nothing is how
 * you lose work to a five-second network blip.
 */
public enum ErrorClass {

    /** A blip: connection reset, 503, deadlock detected. Retry with exponential backoff. */
    TRANSIENT,

    /** The remote asked us to slow down: 429, quota exceeded. Retry, but far less eagerly. */
    RATE_LIMITED,

    /** Retrying cannot help: 422, malformed payload, business rule rejection. Dead-letter now. */
    TERMINAL
}
