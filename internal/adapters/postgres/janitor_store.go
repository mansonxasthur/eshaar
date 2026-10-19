package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

var _ ports.JanitorStore = (*Store)(nil)

// maxAttempts is the number of janitor claims a row gets. It is written as a
// literal in the queries and in migration 0005's partial indexes: a bind
// parameter would let the planner pick a generic plan that cannot prove the
// index predicate, and the index would go unused. The queries are built from
// this constant; the migration repeats the literal, and the EXPLAIN test fails
// if the queries and the indexes drift apart.
const maxAttempts = 3

// claimStale takes the rows in one statement. SKIP LOCKED makes a second
// janitor pass over rows the first is claiming instead of waiting for them.
// The COALESCE expression and the WHERE predicate repeat the recovery index
// of migration 0005 literally.
var claimStale = fmt.Sprintf(`
UPDATE notifications
SET claimed_at = now(), attempts = attempts + 1, updated_at = now()
WHERE id IN (
    SELECT id FROM notifications
    WHERE state = 'pending' AND attempts < %[1]d
      AND COALESCE(claimed_at, created_at) < now() - make_interval(secs => $1::float8)
    ORDER BY COALESCE(claimed_at, created_at)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, idempotency_key, recipient_id, notification_type, channel, content, email, phone,
          last_provider, attempts, claimed_at, created_at`, maxAttempts)

// ClaimStale implements ports.JanitorStore.
func (s *Store) ClaimStale(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error) {
	if limit < 1 {
		return nil, fmt.Errorf("claim stale notifications: limit must be at least 1, got %d", limit)
	}
	res, err := s.pool.Query(ctx, claimStale, staleAfter.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim stale notifications: %w", err)
	}
	defer res.Close()

	var claimed []domain.Notification
	for res.Next() {
		var (
			n       domain.Notification
			channel string
			content []byte
		)
		err := res.Scan(&n.ID, &n.IdempotencyKey, &n.RecipientID, &n.NotificationType, &channel, &content,
			&n.Email, &n.Phone, &n.LastProvider, &n.Attempts, &n.ClaimedAt, &n.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan claimed row: %w", err)
		}
		n.Channel = domain.Channel(channel)
		n.Content = content
		n.State = domain.StatePending
		claimed = append(claimed, n)
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("claim stale notifications: %w", err)
	}
	return claimed, nil
}

// sweepExhausted marks exhausted rows unresolved. It repeats the sweeper index
// of migration 0005 literally.
var sweepExhausted = fmt.Sprintf(`
UPDATE notifications
SET state = 'unresolved', failure_reason = 'recovery attempts exhausted; delivery unknown', updated_at = now()
WHERE id IN (
    SELECT id FROM notifications
    WHERE state = 'pending' AND attempts >= %[1]d
      AND claimed_at < now() - make_interval(secs => $1::float8)
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)`, maxAttempts)

// SweepExhausted implements ports.JanitorStore.
func (s *Store) SweepExhausted(ctx context.Context, staleAfter time.Duration, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("sweep exhausted notifications: limit must be at least 1, got %d", limit)
	}
	tag, err := s.pool.Exec(ctx, sweepExhausted, staleAfter.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("sweep exhausted notifications: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
