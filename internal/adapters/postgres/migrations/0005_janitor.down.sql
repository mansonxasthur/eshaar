DROP INDEX IF EXISTS notifications_exhausted_pending_idx;
DROP INDEX IF EXISTS notifications_recovery_pending_idx;
ALTER TABLE notifications DROP COLUMN attempts;
