package sender

import (
	"context"
	"errors"
	"fmt"
	"math"

	"golang.org/x/time/rate"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// errLimiterWait marks a call that never reached the provider because the
// outbound limiter made it wait too long. The breaker does not count it.
var errLimiterWait = errors.New("outbound rate limit wait")

// rateLimited caps how fast a provider is called. A caller that has to wait
// holds a pool slot, so a slow provider backs the consume loop up into Kafka.
type rateLimited struct {
	ports.Provider
	limiter *rate.Limiter
}

// WithRateLimit wraps p so it is called at most rps times a second. The wait
// counts against the send's timeout.
func WithRateLimit(p ports.Provider, rps float64) ports.Provider {
	return keepStatus(rateLimited{Provider: p, limiter: rate.NewLimiter(rate.Limit(rps), max(1, int(math.Ceil(rps))))}, p)
}

func (r rateLimited) Send(ctx context.Context, n domain.Notification) (string, error) {
	if err := r.limiter.Wait(ctx); err != nil {
		// Nothing was sent, so this is not a provider timeout: the error is
		// transient for the fallback and neutral for the breaker. The text, not
		// the error, is wrapped so the context error cannot match a deadline.
		return "", fmt.Errorf("%w: %w: %s", ports.ErrTransient, errLimiterWait, err.Error())
	}
	return r.Provider.Send(ctx, n)
}
