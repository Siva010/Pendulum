-- Pendulum V3: an explicit CANCELLED state for operator intervention.
--
-- Deleting the row instead would be simpler and wrong: the most common question after an incident
-- is "what happened to job X", and a deleted row answers it with silence. A terminal state keeps
-- the failure chain, the attempt count, and the operator's decision on the record.

ALTER TABLE jobs DROP CONSTRAINT jobs_state_check;

ALTER TABLE jobs ADD CONSTRAINT jobs_state_check
    CHECK (state IN ('PENDING', 'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED'));

-- Replay resets a dead-lettered job's attempt budget, so the original attempt count would be lost.
-- Keeping it is what lets an operator see "this was replayed twice and burned 5 attempts each
-- time" rather than a job that looks like it has only ever failed once.
ALTER TABLE jobs ADD COLUMN replay_count INT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN last_replayed_at TIMESTAMPTZ;

COMMENT ON COLUMN jobs.replay_count IS
    'How many times an operator has replayed this job from a terminal state. Attempt counters reset on replay; this one does not.';
