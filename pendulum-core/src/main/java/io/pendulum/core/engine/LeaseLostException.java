package io.pendulum.core.engine;

import io.pendulum.core.domain.LeaseToken;

import java.util.UUID;

/**
 * Thrown when this worker no longer owns the job it is executing — the lease expired (a long
 * GC pause, a stalled database call, a partitioned network) and the reaper handed it to
 * someone else.
 *
 * <p>The engine never records a retry or a failure for this: another worker now owns the job,
 * and writing anything about it would be exactly the double-write fencing exists to prevent.
 */
public class LeaseLostException extends RuntimeException {

    private final UUID jobId;
    private final LeaseToken token;

    public LeaseLostException(UUID jobId, LeaseToken token) {
        super("lease lost for job " + jobId + " (" + token + ")");
        this.jobId = jobId;
        this.token = token;
    }

    public UUID jobId() { return jobId; }

    public LeaseToken token() { return token; }
}
