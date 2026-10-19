package sender

import (
	"context"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// statusProvider is a wrapped provider that still answers status checks.
type statusProvider struct {
	ports.Provider
	checker ports.StatusChecker
}

func (s statusProvider) Status(ctx context.Context, idempotencyKey string, channel domain.Channel) (string, bool, error) {
	return s.checker.Status(ctx, idempotencyKey, channel)
}

// keepStatus returns wrapped, with the status check of inner if it has one. A
// decorator embeds only ports.Provider, so without this it would hide the
// optional ports.StatusChecker and the sender would skip the check after a
// timeout. Status checks are not rate limited or counted by the breaker.
func keepStatus(wrapped, inner ports.Provider) ports.Provider {
	if c, ok := inner.(ports.StatusChecker); ok {
		return statusProvider{Provider: wrapped, checker: c}
	}
	return wrapped
}
