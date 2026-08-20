package io.pendulum.core.retry;

/** Thrown by a handler to say: this will never succeed, do not retry it. */
public class TerminalJobException extends RuntimeException {

    public TerminalJobException(String message) {
        super(message);
    }

    public TerminalJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
