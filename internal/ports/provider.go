package ports

import (
	"context"

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
