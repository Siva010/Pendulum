-- Pendulum V5: durable workflows.
--
-- A workflow run is executed by an ordinary job, so it inherits leasing, fencing, heartbeats,
-- retries and the dead-letter queue unchanged. What the two tables below add is *memory*: each
-- step's output is committed as it completes, so a crash resumes at the last completed step
-- instead of re-running the whole thing from the beginning.
--
-- That is the entire idea behind durable execution. Without step_results, retrying a five-step
-- workflow that failed at step four re-charges the card in step two.

CREATE TABLE workflow_runs (
    id              UUID        PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    workflow_type   TEXT        NOT NULL,
    input           JSONB       NOT NULL DEFAULT '{}'::jsonb,

    state           TEXT        NOT NULL DEFAULT 'PENDING'
                                CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    -- The resume point: the number of steps known to have completed. Advanced only after a step's
    -- output is durably committed, never before — the ordering is the whole guarantee.
    completed_steps INT         NOT NULL DEFAULT 0,

    -- The job that drives this run. One job per run: the workflow's retries, attempt budget and
    -- dead-lettering are the job's, with no parallel machinery to keep in sync.
    job_id          UUID,

    result          JSONB,
    last_error      TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE TABLE step_results (
    run_id       UUID        NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_index   INT         NOT NULL,
    step_name    TEXT        NOT NULL,
    output       JSONB,
    attempts     INT         NOT NULL DEFAULT 1,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The primary key does the real work. Two workers that both executed step 3 — because one
    -- stalled past its lease and the other took over — race to INSERT, and exactly one wins. The
    -- loser adopts the winner's output rather than overwriting it, so both continue from an
    -- identical state and the run has one authoritative history.
    PRIMARY KEY (run_id, step_index)
);

CREATE INDEX ix_workflow_runs_tenant ON workflow_runs (tenant_id, state, created_at DESC);
CREATE INDEX ix_workflow_runs_job ON workflow_runs (job_id) WHERE job_id IS NOT NULL;

COMMENT ON TABLE step_results IS
    'One committed output per completed step. Replayed on resume so completed work is never repeated; the composite primary key makes a concurrent duplicate execution converge rather than corrupt.';
COMMENT ON COLUMN workflow_runs.completed_steps IS
    'Resume point. Advanced only after the corresponding step output is committed, so a crash between the two re-runs the step rather than skipping it.';
