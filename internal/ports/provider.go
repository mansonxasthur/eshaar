package ports

import (
	"context"
	"errors"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// Provider delivers a notification through an external service. Adding a
// provider or a channel means adding an adapter that implements this.
type Provider interface {
	Name() string

	// Send hands the notification to the provider and returns the provider's
	// message ID once it accepts. Delivery is reported later by webhook. An
	// error's text is stored and logged, so it must not contain the recipient's
	// email or phone number.
	Send(ctx context.Context, n domain.Notification) (providerMessageID string, err error)
}

// Errors a provider wraps (with %w) to say what went wrong, so the sender can
// pick the next step without knowing the provider. An error matching none of
// them is treated as transient.
var (
	// ErrTimeout means the provider did not answer in time. The message may or
	// may not have been taken.
	ErrTimeout = errors.New("provider timed out")

	// ErrRateLimited means the provider refused the call because of its rate
	// limit. The sender switches provider and does not retry.
	ErrRateLimited = errors.New("provider rate limited")

	// ErrTransient means a temporary failure on the provider's side.
	ErrTransient = errors.New("provider transient error")

	// ErrRejected means the provider refused the message outright.
	ErrRejected = errors.New("provider rejected the message")
)

// StatusChecker is implemented by providers that can say whether they already
// took a message, looked up by idempotency key. The sender asks it after a
// timeout, before switching to the other provider. A provider that cannot do
// this simply does not implement it.
type StatusChecker interface {
	// Status returns the provider's message ID and true if it took the message.
	Status(ctx context.Context, idempotencyKey string, channel domain.Channel) (providerMessageID string, found bool, err error)
}
