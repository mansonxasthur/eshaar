-- The two partial indexes of migration 0001 were built for the first janitor
-- design (a 30 s age on never-claimed rows, a 10 s age on claimed ones). The
-- janitor now selects on attempts and on COALESCE(claimed_at, created_at), so
-- these indexes match no query, while every insert and every state change of a
-- pending row still pays to maintain them. The Phase 6 migration creates the
-- indexes the new queries need, with attempts.
DROP INDEX IF EXISTS notifications_unclaimed_pending_idx;
DROP INDEX IF EXISTS notifications_claimed_pending_idx;
