-- Pendulum V4: cron schedules.
--
-- The schedule table holds intent; firing produces ordinary rows in `jobs`, which means cron work
-- inherits leasing, fencing, retries and the dead-letter queue for free. A scheduler that executed
-- work itself would have to reimplement all of it, badly.

CREATE TABLE cron_schedules (
    id              UUID        PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    name            TEXT        NOT NULL,

    cron_expression TEXT        NOT NULL,
    -- An IANA zone id, never a fixed offset. "Europe/London" survives a DST transition and a
    -- government changing the rules; "+01:00" silently means the wrong wall-clock time for half
    -- the year, and tzdata updates cannot fix it.
    timezone        TEXT        NOT NULL,

    job_type        TEXT        NOT NULL,
    queue           TEXT        NOT NULL DEFAULT 'default',
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    priority        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 5,

    enabled         BOOLEAN     NOT NULL DEFAULT true,

    -- What to do about occurrences that elapsed while nobody was ticking.
    misfire_policy  TEXT        NOT NULL DEFAULT 'FIRE_ONCE'
                                CHECK (misfire_policy IN ('SKIP', 'FIRE_ONCE', 'FIRE_ALL')),
    -- FIRE_ALL without a ceiling is a loaded gun: a minutely schedule down for a week wakes up and
    -- enqueues 10,080 jobs in one tick, turning a recovered outage into a new one.
    catch_up_limit  INT         NOT NULL DEFAULT 100,

    -- next_fire_at doubles as the claim: a ticker takes a schedule by pushing this forward, so a
    -- second ticker sees nothing due. Same visibility-timeout trick as the outbox.
    next_fire_at    TIMESTAMPTZ NOT NULL,
    last_fired_at   TIMESTAMPTZ,
    fire_count      BIGINT      NOT NULL DEFAULT 0,
    last_error      TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT cron_schedules_name_unique UNIQUE (tenant_id, name),
    CONSTRAINT cron_schedules_bounds CHECK (catch_up_limit > 0 AND max_attempts > 0)
);

CREATE INDEX ix_cron_due ON cron_schedules (next_fire_at) WHERE enabled;

COMMENT ON COLUMN cron_schedules.timezone IS
    'IANA zone id. Firing is computed in local wall-clock time for this zone, so a 02:30 daily job stays at 02:30 across DST rather than drifting an hour twice a year.';
COMMENT ON COLUMN cron_schedules.misfire_policy IS
    'SKIP: forget occurrences missed during downtime. FIRE_ONCE: run the most recent missed occurrence, then resume. FIRE_ALL: run every missed occurrence, bounded by catch_up_limit.';
