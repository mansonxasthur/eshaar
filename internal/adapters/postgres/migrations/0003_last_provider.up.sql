-- The last provider a send actually called on a row that ended without an
-- outcome (nothing accepted it, and no provider timed out), so the janitor can
-- resume the fallback chain from there. It is not the provider that accepted a
-- row: that stays in provider.
ALTER TABLE notifications ADD COLUMN last_provider text;
