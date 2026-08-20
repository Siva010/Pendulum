package io.pendulum.core.store;

/** Wraps the checked {@code SQLException} at the persistence boundary. */
public class JobStoreException extends RuntimeException {

    public JobStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
