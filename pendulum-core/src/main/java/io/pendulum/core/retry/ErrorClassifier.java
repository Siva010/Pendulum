package io.pendulum.core.retry;

/**
 * Decides what kind of failure a thrown exception represents.
 *
 * <p>Applications override this: only the application knows that its
 * {@code PaymentDeclinedException} is terminal while its
 * {@code PaymentGatewayTimeoutException} is not.
 */
@FunctionalInterface
public interface ErrorClassifier {

    /** How far down a cause chain to look before giving up. */
    int MAX_CAUSE_DEPTH = 16;

    ErrorClass classify(Throwable error);

    /**
     * The default: explicit marker exceptions win, obvious programming errors are terminal,
     * and anything unrecognised is treated as transient.
     *
     * <p>Unrecognised-means-transient is a deliberate bias. The cost of retrying something
     * that will never succeed is bounded by {@code max_attempts} and ends in the DLQ; the
     * cost of dead-lettering something that would have succeeded is lost work that a human
     * has to notice.
     */
    static ErrorClassifier defaults() {
        return error -> {
            // Bounded rather than "walk until null": cause chains can be cyclic (A caused by B
            // caused by A is legal to construct), and an unbounded walk over one hangs the worker
            // thread inside the failure path, which is the worst possible place to hang.
            Throwable cause = error;
            for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
                if (cause instanceof TerminalJobException) return ErrorClass.TERMINAL;
                if (cause instanceof RateLimitedException) return ErrorClass.RATE_LIMITED;
                if (cause instanceof IllegalArgumentException) return ErrorClass.TERMINAL;
                if (cause instanceof NullPointerException) return ErrorClass.TERMINAL;
                cause = cause.getCause();
            }
            return ErrorClass.TRANSIENT;
        };
    }
}
