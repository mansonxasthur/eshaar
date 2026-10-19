package ports

import (
	"context"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// NotificationStore saves notifications and records the outcome of a send.
type NotificationStore interface {
	// InsertPending saves the rows in one statement and returns only those this
	// call created. A row whose idempotency key and channel already exist is
	// treated as already saved and left out of the result.
	InsertPending(ctx context.Context, rows []domain.Notification) ([]domain.Notification, error)

	// MarkAccepted moves a pending row to accepted, recording which provider
	// took it. It does nothing if the row is no longer pending.
	MarkAccepted(ctx context.Context, id, provider, providerMessageID string) error

	// MarkFailed moves a pending row to failed with a reason. It does nothing if
	// the row is no longer pending.
	MarkFailed(ctx context.Context, id, reason string) error
}
