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

// Pair is the two providers behind a channel. The sender tries A first and
// falls back to B. B may be nil, and then there is no fallback.
type Pair struct {
	A, B ports.Provider
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

// deliver runs the fallback flow for one notification and returns its outcome:
//
//   - A accepts, or B accepts after A failed: accepted.
//   - A rate-limit answer or a failure switches to B at once, with no retry.
//   - A timeout first asks A, by idempotency key, if it took the message and
//     accepts A's message ID if so. If A cannot answer, it switches to B and
//     accepts the possible duplicate.
//   - Nothing accepted and some attempt timed out :
//     unresolved, because the message may have been delivered.
//   - Nothing accepted and every attempt was refused outright: failed.
//
// The whole flow runs under SendTimeout; each provider call also has its own
// timeout.
func (s *Sender) deliver(ctx context.Context, log *slog.Logger, n domain.Notification, pair Pair) outcome {
	// The status check hangs off the caller's context, not the send deadline: A
	// may have used all of that deadline, and the check is how we learn that it
	// took the message anyway. It has its own StatusTimeout.
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, s.cfg.SendTimeout)
	defer cancel()

	steps := []struct {
		name     string
		provider ports.Provider
		timeout  time.Duration
	}{
		{"a", pair.A, s.cfg.ProviderATimeout},
		{"b", pair.B, s.cfg.ProviderBTimeout},
	}

	var reasons []string
	uncertain := false
	for _, step := range steps {
		if step.provider == nil {
			continue
		}
		if ctx.Err() != nil {
			log.WarnContext(ctx, "no time left for provider", "slot", step.name, "provider", step.provider.Name())
			reasons = append(reasons, step.provider.Name()+": no time left")
			continue
		}
		id, err := s.attempt(ctx, log, n, step.name, step.provider, step.timeout)
		if err == nil {
			return outcome{state: domain.StateAccepted, provider: step.provider.Name(), messageID: id}
		}
		reasons = append(reasons, fmt.Sprintf("%s: %v", step.provider.Name(), err))

		if kindOf(err) != kindTimeout {
			continue
		}
		uncertain = true
		// Only A is asked: after B there is nowhere left to switch to.
		if step.name == "a" {
			if id, found := s.checkStatus(parent, log, n, step.provider); found {
				return outcome{state: domain.StateAccepted, provider: step.provider.Name(), messageID: id}
			}
		}
	}

	if len(reasons) == 0 {
		return outcome{state: domain.StateFailed, reason: "no provider for channel " + string(n.Channel)}
	}
	reason := strings.Join(reasons, "; ")
	if uncertain {
		return outcome{state: domain.StateUnresolved, reason: reason}
	}
	return outcome{state: domain.StateFailed, reason: reason}
}

// attempt makes one provider call under its own timeout and logs how it ended.
func (s *Sender) attempt(ctx context.Context, log *slog.Logger, n domain.Notification, slot string, p ports.Provider, timeout time.Duration) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	id, err := p.Send(callCtx, n)

	k := kindOf(err)
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
	}
	attrs := []any{"slot", slot, "provider", p.Name(), "outcome", string(k), "duration", time.Since(start)}
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
