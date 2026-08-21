-- Pendulum V2: the transactional outbox, for effects that must leave the database.
--
-- Read the note in the README before reaching for this table. When the destination is Pendulum
-- itself and your business data shares this database, you do not need an outbox: inserting into
-- `jobs` inside the caller's transaction already is the pattern, with `jobs` playing the outbox
-- role. A second table would add a hop and a relay to achieve exactly what one INSERT already
-- guarantees.
--
-- This table is for the case that genuinely cannot be done transactionally: publishing to Kafka,
-- calling a webhook, sending to a payment provider. There is no distributed transaction spanning
-- Postgres and Kafka, so the durable record of intent is committed here alongside the business
-- write, and a relay makes the external call afterwards with at-least-once delivery.

CREATE TABLE outbox (
    id              UUID        PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,

    -- Where this is going: a Kafka topic, a webhook name, a provider id. The relay dispatches on it.
    destination     TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    headers         JSONB       NOT NULL DEFAULT '{}'::jsonb,

    state           TEXT        NOT NULL DEFAULT 'PENDING'
                                CHECK (state IN ('PENDING', 'PUBLISHED', 'DEAD_LETTERED')),

    -- next_attempt_at doubles as the visibility timeout. A relay claims a row by pushing this
    -- forward; if the relay dies mid-publish, the timeout lapses and another relay picks it up.
    -- Same discipline as job leasing, for the same reason: recovery cannot depend on the dead
    -- process doing anything.
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 10,

    -- Deduplication key for the consumer. Delivery is at-least-once and cannot be otherwise, so
    -- the honest thing is to hand every consumer a stable id to dedupe on rather than pretend.
    message_key     TEXT,

    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,

    CONSTRAINT outbox_attempt_bounds CHECK (attempts >= 0 AND max_attempts > 0)
);

-- The relay's only query. Partial so it stays small as published rows accumulate.
CREATE INDEX ix_outbox_pending
    ON outbox (next_attempt_at, id)
    WHERE state = 'PENDING';

CREATE INDEX ix_outbox_tenant_state ON outbox (tenant_id, state, created_at DESC);

-- Same churn profile as the jobs table, same reasoning: rows are written, updated a few times,
-- and never read again.
ALTER TABLE outbox SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);

COMMENT ON TABLE outbox IS
    'Durable record of an intent to cause an effect outside this database, committed in the same transaction as the business write that justified it. Drained by OutboxRelay with at-least-once delivery.';
