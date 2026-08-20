package io.pendulum.core.engine;

import io.pendulum.core.domain.Job;
import io.pendulum.core.domain.LeaseToken;

import java.util.function.BooleanSupplier;

/**
 * What a handler is given: the job, and a way to ask whether we still hold its lease.
 *
 * <p>The lease check is the cooperative half of fencing. The engine enforces fencing on the
 * write path unconditionally — a stale worker's completion is rejected by the database — but
 * that only prevents a stale <em>result</em>, not stale <em>side effects</em>. A handler doing
 * something long and externally visible should call {@link #checkLease()} between steps so it
 * stops early instead of calling a payment API on a lease it lost thirty seconds ago.
 */
public record JobContext(Job job, LeaseToken leaseToken, BooleanSupplier leaseHolder) {

    /** The raw JSON payload. Bind it to a type you name explicitly — never native deserialization. */
    public String payload() {
        return job.payload();
    }

    public int attempt() {
        return job.attempt();
    }

    /** True while this worker still owns the job. */
    public boolean leaseHeld() {
        return leaseHolder.getAsBoolean();
    }

    /** Abort the handler if the lease has been lost. */
    public void checkLease() {
        if (!leaseHeld()) {
            throw new LeaseLostException(job.id(), leaseToken);
        }
    }
}
