package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Route is one provider in a channel's fallback chain, with the limit for a
// single call to it. A channel's chain is ordered: the first route is tried
// first and each later one is the fallback for the one before it.
type Route struct {
	Provider ports.Provider
	Timeout  time.Duration
}

// kind says how a provider call ended.
type kind string

const (
	kindOK          kind = "ok"
	kindRateLimited kind = "rate_limited"
	kindRejected    kind = "rejected"
	kindTimeout     kind = "timeout"
	kindTransient   kind = "transient"
)

// kindOf classifies a provider error. An error that matches no known kind,
// including an open circuit breaker, is transient.
func kindOf(err error) kind {
	switch {
	case err == nil:
		return kindOK
	case errors.Is(err, ports.ErrRateLimited):
		return kindRateLimited
	case errors.Is(err, ports.ErrRejected):
		return kindRejected
	case errors.Is(err, ports.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return kindTimeout
	}
	return kindTransient
}

// outcome is what a channel send ends in.
type outcome struct {
	state     domain.State // accepted, failed or unresolved; pending means no outcome to write
	provider  string       // accepted only
	messageID string       // accepted only
	reason    string       // failed, unresolved and pending

	// lastTried is, for a pending outcome, the last provider that was actually
	// called, empty if none was. The janitor resumes the chain from it.
	lastTried string
}

// deliver runs the fallback flow for one notification over its channel's chain
// and returns its outcome:
//
//   - A provider accepts: accepted, by the first one that does.
//   - A rate-limit answer or a failure switches to the next provider at once,
//     with no retry.
//   - A timeout first asks that provider, by idempotency key, if it took the
//     message and accepts its message ID if so. If it cannot answer, it switches
//     to the next provider and accepts the possible duplicate. The last provider
//     is not asked: there is nowhere left to switch to.
//   - Nothing accepted and some attempt timed out: unresolved, because the
//     message may have been delivered.
//   - Nothing accepted, no timeout, but some provider was never tried (its
//     breaker was open or the deadline had passed): no outcome, and the row stays
//     pending so the janitor re-drives it. Marking it failed would lose a
//     message that nothing was ever sent for. The last provider actually called
//     is returned so the janitor can resume from it.
//   - Nothing accepted and every provider was tried and refused it outright:
//     failed.
//
// The flow starts at chain[first], so a re-drive can resume where the sender
// stopped, and wraps around to the providers before it; attempt numbers in the
// log are positions in the whole chain. The whole flow runs under SendTimeout; each provider call also has its own
// timeout from its route.
func (d *Deliverer) deliver(ctx context.Context, log *slog.Logger, n domain.Notification, chain []Route, first int) outcome {
	if len(chain) == 0 {
		return outcome{state: domain.StateFailed, reason: "no provider for channel " + string(n.Channel)}
	}

	// The status check hangs off the caller's context, not the send deadline: a
	// provider may have used all of that deadline, and the check is how we learn
	// that it took the message anyway. It has its own StatusTimeout.
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, d.cfg.SendTimeout)
	defer cancel()

	var reasons []string
	uncertain, skipped := false, false
	var lastTried string
	// The order is chain[first:], then the providers before first. A row stays
	// pending when a provider was skipped, and that provider can sit before the
	// one the sender last called, so a resumed run ends by trying those too:
	// failed must only mean every provider refused the message.
	order := make([]int, 0, len(chain))
	for k := range chain {
		order = append(order, (first+k)%len(chain))
	}
	for pos, i := range order {
		route := chain[i]
		name := route.Provider.Name()
		if ctx.Err() != nil {
			log.WarnContext(ctx, "no time left for provider", "attempt", i+1, "provider", name)
			reasons = append(reasons, name+": no time left")
			skipped = true
			continue
		}
		id, err := d.attempt(ctx, log, n, i+1, route)
		if !errors.Is(err, ErrCircuitOpen) {
			lastTried = name // an open breaker means the provider was never called
		}
		if err == nil {
			return outcome{state: domain.StateAccepted, provider: name, messageID: id}
		}
		reasons = append(reasons, fmt.Sprintf("%s: %v", name, err))
		if errors.Is(err, ErrCircuitOpen) {
			skipped = true // nothing reached the provider
		}

		if kindOf(err) != kindTimeout {
			continue
		}
		uncertain = true
		if pos < len(order)-1 {
			if id, found := d.checkStatus(parent, log, n, route.Provider); found {
				return outcome{state: domain.StateAccepted, provider: name, messageID: id}
			}
		}
	}

	reason := strings.Join(reasons, "; ")
	switch {
	case uncertain:
		return outcome{state: domain.StateUnresolved, reason: reason}
	case skipped:
		return outcome{state: domain.StatePending, reason: reason, lastTried: lastTried}
	}
	return outcome{state: domain.StateFailed, reason: reason}
}

// attempt makes one provider call under its own timeout and logs how it ended.
func (d *Deliverer) attempt(ctx context.Context, log *slog.Logger, n domain.Notification, position int, route Route) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, route.Timeout)
	defer cancel()
	start := time.Now()
	id, err := route.Provider.Send(callCtx, n)

	k := kindOf(err)
	// A success is the common case on a hot path, so it is only a debug line.
	level := slog.LevelDebug
	if err != nil {
		level = slog.LevelWarn
	}
	attrs := []any{"attempt", position, "provider", route.Provider.Name(), "outcome", string(k), "duration", time.Since(start)}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	log.Log(ctx, level, "provider attempt", attrs...)
	return id, err
}

// checkStatus asks p whether it took the message n, within StatusTimeout. It
// returns false when p cannot say, so the caller switches provider anyway.
func (d *Deliverer) checkStatus(ctx context.Context, log *slog.Logger, n domain.Notification, p ports.Provider) (string, bool) {
	checker, ok := p.(ports.StatusChecker)
	if !ok {
		log.InfoContext(ctx, "provider cannot check status", "provider", p.Name())
		return "", false
	}
	checkCtx, cancel := context.WithTimeout(ctx, d.cfg.StatusTimeout)
	defer cancel()
	id, found, err := checker.Status(checkCtx, n.IdempotencyKey, n.Channel)
	switch {
	case err != nil:
		log.WarnContext(ctx, "status check failed", "provider", p.Name(), "error", err)
		return "", false
	case !found:
		log.InfoContext(ctx, "status check: message not taken", "provider", p.Name())
		return "", false
	}
	log.InfoContext(ctx, "status check: message taken", "provider", p.Name(), "provider_message_id", id)
	return id, true
}
