-- Pendulum V1: the jobs table and the lease/fencing substrate.
--
-- Design notes worth defending:
--  * State is TEXT + CHECK, not an enum type. Postgres enums cannot have values
--    removed and ALTER TYPE ... ADD VALUE cannot run inside a transaction before
--    PG12; a CHECK constraint is trivially migratable.
--  * Fencing tokens come from one global sequence. Global monotonicity implies
--    per-job monotonicity, which is all the fencing argument needs, and nextval()
--    is transaction-safe without locking.
--  * All timestamps are set from the database clock (now()), never from a worker.
--    Two workers with 3s of drift produce nondeterministic lease behaviour.

CREATE SEQUENCE pendulum_lease_token_seq AS BIGINT START 1;

CREATE TABLE jobs (
    id                UUID        PRIMARY KEY,
    tenant_id         TEXT        NOT NULL,
    queue             TEXT        NOT NULL,
    job_type          TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,

    state             TEXT        NOT NULL
                                  CHECK (state IN ('PENDING','LEASED','RUNNING',
                                                   'SUCCEEDED','FAILED','DEAD_LETTERED')),
    priority          INT         NOT NULL DEFAULT 0,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 5,

    -- Lease / fencing. lease_token is deliberately NOT cleared on completion or
    -- reaping: keeping the last token is useful forensics, and staleness is already
    -- rejected by the state predicate in the conditional writes.
    lease_token       BIGINT,
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,

    idempotency_key   TEXT,
    last_error        TEXT,
    completed_at      TIMESTAMPTZ,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_attempt_bounds CHECK (attempt >= 0 AND max_attempts > 0)
);

-- The hot path. Every dispatch poll is:
--   WHERE state = 'PENDING' AND queue = ? AND run_at <= now()
--   ORDER BY priority DESC, run_at, id
-- A partial index keeps the index small even when the table holds millions of
-- terminal rows, and lets the planner satisfy the ORDER BY without a sort.
CREATE INDEX ix_jobs_dispatch
    ON jobs (queue, priority DESC, run_at, id)
    WHERE state = 'PENDING';

-- The reaper's scan: expired leases, oldest first.
CREATE INDEX ix_jobs_expired_leases
    ON jobs (lease_expires_at)
    WHERE state IN ('LEASED','RUNNING');

-- Enqueue-level deduplication. The unique constraint does the real work; the
-- application never checks-then-inserts (that race is the classic bug).
CREATE UNIQUE INDEX ux_jobs_idempotency
    ON jobs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Admin/observability access paths.
CREATE INDEX ix_jobs_tenant_state ON jobs (tenant_id, state, created_at DESC);

-- A queue table is the highest-churn table in any schema: every job is updated
-- 3-5 times and then never read again. At the default scale factor of 0.2,
-- autovacuum waits for 20% of the table to be dead before running, by which time
-- the dispatch index is bloated and the polling query has quietly degraded.
ALTER TABLE jobs SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_limit    = 1000
);

COMMENT ON COLUMN jobs.lease_token IS
    'Monotonic fencing token. Every write that reports the outcome of an execution is conditional on holding this exact token, which is what makes lease expiry safe when a slow worker (long GC pause) comes back to life.';
COMMENT ON COLUMN jobs.attempt IS
    'Incremented at claim time, not at failure time. A poison pill that kills the worker process therefore still consumes an attempt and cannot retry forever.';
