package sender

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// ErrCircuitOpen is returned without calling the provider while its breaker is
// open. It is an immediate transient failure, not a timeout, so there is
// nothing to check status on.
var ErrCircuitOpen = fmt.Errorf("circuit breaker open: %w", ports.ErrTransient)

// defaultOpenFor is used when BreakerConfig.OpenFor is not positive.
const defaultOpenFor = 30 * time.Second

// BreakerConfig tunes a provider's circuit breaker.
type BreakerConfig struct {
	// FailureThreshold is how many failures in a row open the breaker.
	FailureThreshold int

	// OpenFor is how long the breaker stays open before letting one probe call
	// through. It defaults to 30 seconds.
	OpenFor time.Duration

	// Now is the clock. It defaults to time.Now.
	Now func() time.Time
}

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

// breaker stops calling a provider that keeps failing. Only transient errors
// and timeouts count as failures. A rate-limit or rejection means the provider
// answered, so it counts as a healthy response. A cancelled context is the
// caller's doing and counts as neither.
type breaker struct {
	ports.Provider
	cfg BreakerConfig

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time
	probing  bool

	// generation changes on every state change, so a call that was already in
	// flight when the state changed cannot move the breaker when it returns.
	generation int
}

// WithBreaker wraps p in a circuit breaker. After FailureThreshold failures in
// a row it fails fast with ErrCircuitOpen, and after OpenFor it lets one probe
// through: a healthy answer closes the breaker, a failure opens it again.
//
// Put the breaker inside the rate limit, WithRateLimit(WithBreaker(p, cfg), rps),
// so the limiter's own wait errors are not counted as provider failures.
func WithBreaker(p ports.Provider, cfg BreakerConfig) ports.Provider {
	cfg.FailureThreshold = max(1, cfg.FailureThreshold)
	if cfg.OpenFor <= 0 {
		cfg.OpenFor = defaultOpenFor
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return keepStatus(&breaker{Provider: p, cfg: cfg}, p)
}

func (b *breaker) Send(ctx context.Context, n domain.Notification) (string, error) {
	gen, ok := b.allow()
	if !ok {
		return "", ErrCircuitOpen
	}
	// A panic in the provider must not leave a probe pending forever.
	defer func() {
		if r := recover(); r != nil {
			b.record(gen, ErrCircuitOpen)
			panic(r)
		}
	}()
	id, err := b.Provider.Send(ctx, n)
	b.record(gen, err)
	return id, err
}

// allow reports whether a call may go to the provider, and the generation it
// runs in.
func (b *breaker) allow() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		if b.cfg.Now().Sub(b.openedAt) < b.cfg.OpenFor {
			return 0, false
		}
		b.transition(halfOpen)
		b.probing = true
	case halfOpen:
		if b.probing {
			return 0, false
		}
		b.probing = true
	}
	return b.generation, true
}

// record applies a call's result. It ignores a call from an older generation.
func (b *breaker) record(gen int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen != b.generation {
		return
	}
	switch {
	case errors.Is(err, context.Canceled):
		b.probing = false // let the next call probe instead
	case isProviderFailure(err):
		b.failures++
		if b.state == halfOpen || b.failures >= b.cfg.FailureThreshold {
			b.transition(open)
			b.openedAt = b.cfg.Now()
		}
	default:
		b.failures = 0
		if b.state != closed {
			b.transition(closed)
		}
	}
}

func (b *breaker) transition(to breakerState) {
	b.state, b.probing = to, false
	b.generation++
}

// isProviderFailure reports whether err counts against the breaker. An error
// that matches no known kind is treated as transient.
func isProviderFailure(err error) bool {
	switch {
	case err == nil,
		errors.Is(err, ports.ErrRateLimited),
		errors.Is(err, ports.ErrRejected):
		return false
	}
	return true
}
