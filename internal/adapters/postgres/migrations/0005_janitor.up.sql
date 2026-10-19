-- Janitor attempts: how many times the recovery janitor has claimed the row.
-- The sender's own first send does not count, so a row gets 3 re-drives.
ALTER TABLE notifications ADD COLUMN attempts integer NOT NULL DEFAULT 0;

-- Recovery janitor claim: pending rows with attempts left whose last chance to
-- finish (the claim, or the insert if never claimed) is old enough.
--   WHERE state = 'pending' AND attempts < 3
--     AND COALESCE(claimed_at, created_at) < now() - 10s
-- The query must repeat the predicate and the expression literally, or the
-- planner cannot use the index.
CREATE INDEX notifications_recovery_pending_idx
    ON notifications ((COALESCE(claimed_at, created_at)))
    WHERE state = 'pending' AND attempts < 3;

-- Sweeper: pending rows that used all their attempts.
--   WHERE state = 'pending' AND attempts >= 3 AND claimed_at < now() - 10s
CREATE INDEX notifications_exhausted_pending_idx
    ON notifications (claimed_at)
    WHERE state = 'pending' AND attempts >= 3;
