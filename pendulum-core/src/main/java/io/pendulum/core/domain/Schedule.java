package io.pendulum.core.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * When a job becomes eligible to run.
 *
 * <p>The reason this is a type and not just an {@code Instant} is clock trust. "In five
 * minutes" resolved on a worker whose clock is three seconds off produces a different
 * answer than the same expression resolved on the database, and the database is the only
 * clock every participant agrees on. Keeping the <em>intent</em> (now / after a delay /
 * at an instant) intact until the INSERT lets the store resolve delays against
 * {@code now()} in SQL rather than against {@code Instant.now()} in the JVM.
 */
public sealed interface Schedule {

    record Now() implements Schedule {}

    record After(Duration delay) implements Schedule {
        public After {
            if (delay == null || delay.isNegative()) {
                throw new IllegalArgumentException("delay must be non-negative");
            }
        }
    }

    /** An absolute instant. Only correct when the instant genuinely is absolute — a
     *  user-chosen "send at 09:00 on the 3rd" that has already been resolved to UTC. */
    record At(Instant when) implements Schedule {
        public At {
            if (when == null) throw new IllegalArgumentException("when is required");
        }
    }

    Schedule NOW = new Now();

    static Schedule now() { return NOW; }
    static Schedule after(Duration delay) { return new After(delay); }
    static Schedule at(Instant when) { return new At(when); }
}
