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
	state     domain.State // accepted, failed or unresolved
	provider  string       // accepted only
	messageID string       // accepted only
	reason    string       // failed and unresolved
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
//   - Nothing accepted and every attempt was refused outright, or never made
//     for lack of time: failed.
//
// The whole flow runs under SendTimeout; each provider call also has its own
// timeout from its route.
func (s *Sender) deliver(ctx context.Context, log *slog.Logger, n domain.Notification, chain []Route) outcome {
	if len(chain) == 0 {
		return outcome{state: domain.StateFailed, reason: "no provider for channel " + string(n.Channel)}
	}

	// The status check hangs off the caller's context, not the send deadline: a
	// provider may have used all of that deadline, and the check is how we learn
	// that it took the message anyway. It has its own StatusTimeout.
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()

	var reasons []string
	uncertain := false
	for i, route := range chain {
		name := route.Provider.Name()
		if ctx.Err() != nil {
			log.WarnContext(ctx, "no time left for provider", "attempt", i+1, "provider", name)
			reasons = append(reasons, name+": no time left")
			continue
		}
		id, err := s.attempt(ctx, log, n, i+1, route)
		if err == nil {
			return outcome{state: domain.StateAccepted, provider: name, messageID: id}
		}
		reasons = append(reasons, fmt.Sprintf("%s: %v", name, err))

		if kindOf(err) != kindTimeout {
			continue
		}
		uncertain = true
		if i < len(chain)-1 {
			if id, found := s.checkStatus(parent, log, n, route.Provider); found {
				return outcome{state: domain.StateAccepted, provider: name, messageID: id}
			}
		}
	}

	reason := strings.Join(reasons, "; ")
	if uncertain {
		return outcome{state: domain.StateUnresolved, reason: reason}
	}
	return outcome{state: domain.StateFailed, reason: reason}
}

// attempt makes one provider call under its own timeout and logs how it ended.
func (s *Sender) attempt(ctx context.Context, log *slog.Logger, n domain.Notification, position int, route Route) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, route.Timeout)
	defer cancel()
	start := time.Now()
	id, err := route.Provider.Send(callCtx, n)

	k := kindOf(err)
	level := slog.LevelInfo
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
func (s *Sender) checkStatus(ctx context.Context, log *slog.Logger, n domain.Notification, p ports.Provider) (string, bool) {
	checker, ok := p.(ports.StatusChecker)
	if !ok {
		log.InfoContext(ctx, "provider cannot check status", "provider", p.Name())
		return "", false
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.cfg.StatusTimeout)
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
