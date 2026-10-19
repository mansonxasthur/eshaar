package sender

import (
	"context"
	"math"

	"golang.org/x/time/rate"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// rateLimited caps how fast a provider is called. A caller that has to wait
// holds a pool slot, so a slow provider backs the consume loop up into Kafka.
type rateLimited struct {
	ports.Provider
	limiter *rate.Limiter
}

// WithRateLimit wraps p so it is called at most rps times a second. The wait
// counts against the send's timeout.
func WithRateLimit(p ports.Provider, rps float64) ports.Provider {
	return rateLimited{Provider: p, limiter: rate.NewLimiter(rate.Limit(rps), max(1, int(math.Ceil(rps))))}
}

func (r rateLimited) Send(ctx context.Context, n domain.Notification) (string, error) {
	if err := r.limiter.Wait(ctx); err != nil {
		return "", err
	}
	return r.Provider.Send(ctx, n)
}
