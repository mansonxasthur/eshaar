CREATE INDEX IF NOT EXISTS notifications_unclaimed_pending_idx
    ON notifications (created_at)
    WHERE state = 'pending' AND claimed_at IS NULL;

CREATE INDEX IF NOT EXISTS notifications_claimed_pending_idx
    ON notifications (claimed_at)
    WHERE state = 'pending' AND claimed_at IS NOT NULL;
