package io.pendulum.core.domain;

/**
 * A monotonically increasing fencing token, handed out by the database sequence at
 * claim time.
 *
 * <p>This exists as a type rather than a bare {@code long} because it is the one value
 * in the system whose confusion with another number is a correctness bug: every write
 * that reports the outcome of an execution is conditional on the job still carrying
 * this exact token.
 */
public record LeaseToken(long value) implements Comparable<LeaseToken> {

    public LeaseToken {
        if (value <= 0) {
            throw new IllegalArgumentException("lease token must be positive, got " + value);
        }
    }

    @Override
    public int compareTo(LeaseToken other) {
        return Long.compare(value, other.value);
    }

    @Override
    public String toString() {
        return "lease#" + value;
    }
}
