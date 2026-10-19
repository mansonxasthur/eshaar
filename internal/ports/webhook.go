package ports

import (
	"context"
	"errors"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// WebhookStore saves provider events and applies their final state to the
// notifications.
type WebhookStore interface {
	// RecordEvents saves the events in one transaction and applies each new
	// event's final state to the notification it resolves to. An event whose
	// (provider, event id) already exists is skipped silently. An event that
	// resolves to no notification, or to one that is not yet accepted, is still
	// saved, and ResolvePending links it later. A state the row cannot move to,
	// because it is already final or still pending, is not an error.
	//
	// Events are validated before they reach the store, so a batch holds only
	// well-formed ones. The first event stored for a (provider, event id) wins:
	// a retry carrying another status is skipped, and the state machine keeps
	// a final state final anyway. Events with no event id are never skipped, so
	// a retry of one applies its state again, which is a no-op.
	RecordEvents(ctx context.Context, events []domain.WebhookEvent) error

	// ResolvePending links up to limit saved events that had no notification,
	// oldest first, and applies their state where the row can now take it. It
	// returns how many it linked.
	ResolvePending(ctx context.Context, limit int) (resolved int, err error)
}

// Errors EventReceiver returns, so the HTTP adapter can pick a status code
// without knowing how events are checked.
var (
	// ErrUnknownProvider means no webhook secret is configured for the provider.
	// The HTTP adapter must answer it exactly as ErrInvalidSignature, so a
	// caller cannot tell configured providers from unconfigured ones.
	ErrUnknownProvider = errors.New("unknown webhook provider")

	// ErrInvalidSignature means the signature does not match the body.
	ErrInvalidSignature = errors.New("invalid webhook signature")

	// ErrInvalidEvent means the body is not a well-formed event.
	ErrInvalidEvent = errors.New("invalid webhook event")
)

// EventReceiver takes a provider's webhook as received over HTTP.
type EventReceiver interface {
	// Receive checks the signature over the raw body, parses the event and
	// returns nil only once it is stored, so the provider is told to retry
	// anything that was not. Errors wrapping ErrUnknownProvider,
	// ErrInvalidSignature or ErrInvalidEvent are the sender's fault; any other
	// error is ours.
	Receive(ctx context.Context, provider string, body []byte, signature string) error
}
