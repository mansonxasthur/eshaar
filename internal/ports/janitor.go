package ports

import (
	"context"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// JanitorStore is what the recovery janitor and the sweeper need from storage.
// The re-drive writes its outcome through NotificationStore, whose writes only
// touch rows still pending.
type JanitorStore interface {
	// ClaimStale atomically takes up to limit pending rows that still have
	// attempts left and whose last chance to finish (claimed_at, or created_at
	// if never claimed) is older than staleAfter. Each claimed row has claimed_at
	// set and attempts incremented in the same statement, so two callers never
	// take the same row. When more rows are stale than limit, the oldest are
	// the ones claimed; the order of the returned slice is not defined. The rows
	// carry what a re-drive needs (identity, content, contact, LastProvider,
	// Attempts, ClaimedAt, CreatedAt); other fields are left zero. limit must be
	// at least 1.
	ClaimStale(ctx context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error)

	// SweepExhausted marks up to limit pending rows that used all their attempts,
	// and whose last claim is older than staleAfter, as unresolved with a reason.
	// It returns how many rows it marked. limit must be at least 1.
	SweepExhausted(ctx context.Context, staleAfter time.Duration, limit int) (int, error)
}
