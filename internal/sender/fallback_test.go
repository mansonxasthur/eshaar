package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// stub is a provider whose Send is scripted. It has no status check.
type stub struct {
	name  string
	send  func(ctx context.Context, n domain.Notification) (string, error)
	calls atomic.Int32
}

func (p *stub) Name() string { return p.name }
func (p *stub) Send(ctx context.Context, n domain.Notification) (string, error) {
	p.calls.Add(1)
	return p.send(ctx, n)
}

// checking adds a status check to a stub.
type checking struct {
	*stub
	status func(ctx context.Context, key string, ch domain.Channel) (string, bool, error)
	checks atomic.Int32
}

func (p *checking) Status(ctx context.Context, key string, ch domain.Channel) (string, bool, error) {
	p.checks.Add(1)
	return p.status(ctx, key, ch)
}

func accepts(id string) func(context.Context, domain.Notification) (string, error) {
	return func(context.Context, domain.Notification) (string, error) { return id, nil }
}

func fails(err error) func(context.Context, domain.Notification) (string, error) {
	return func(context.Context, domain.Notification) (string, error) { return "", err }
}

var (
	errTimeout   = fmt.Errorf("%w: no answer", ports.ErrTimeout)
	errRejected  = fmt.Errorf("%w: bad number", ports.ErrRejected)
	errRateLimit = fmt.Errorf("%w: slow down", ports.ErrRateLimited)
	errDown      = fmt.Errorf("%w: 503", ports.ErrTransient)
)

func fallbackConfig() Config {
	cfg := testConfig()
	cfg.SendTimeout = time.Second
	cfg.ProviderATimeout = 500 * time.Millisecond
	cfg.ProviderBTimeout = 500 * time.Millisecond
	cfg.StatusTimeout = 100 * time.Millisecond
	return cfg
}

func deliver(cfg Config, pair Pair) outcome {
	s := &Sender{cfg: cfg}
	n := domain.Notification{ID: "id1", IdempotencyKey: "k1", Channel: domain.ChannelSMS}
	return s.deliver(context.Background(), slog.New(slog.DiscardHandler), n, pair)
}

func TestFallbackSwitchesToBOnFailure(t *testing.T) {
	for name, err := range map[string]error{"transient": errDown, "rejected": errRejected, "unclassified": errors.New("boom")} {
		t.Run(name, func(t *testing.T) {
			a, b := &stub{name: "a", send: fails(err)}, &stub{name: "b", send: accepts("b-1")}
			out := deliver(fallbackConfig(), Pair{A: a, B: b})
			if out.state != domain.StateAccepted || out.provider != "b" || out.messageID != "b-1" {
				t.Errorf("outcome = %+v", out)
			}
			if a.calls.Load() != 1 {
				t.Errorf("A called %d times, want 1", a.calls.Load())
			}
		})
	}
}

func TestFallbackAAccepts(t *testing.T) {
	a, b := &stub{name: "a", send: accepts("a-1")}, &stub{name: "b", send: accepts("b-1")}
	out := deliver(fallbackConfig(), Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "a" || b.calls.Load() != 0 {
		t.Errorf("outcome = %+v, B calls %d", out, b.calls.Load())
	}
}

func TestFallbackRateLimitSwitchesWithoutRetry(t *testing.T) {
	a, b := &stub{name: "a", send: fails(errRateLimit)}, &stub{name: "b", send: accepts("b-1")}
	out := deliver(fallbackConfig(), Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "b" {
		t.Errorf("outcome = %+v", out)
	}
	if a.calls.Load() != 1 {
		t.Errorf("A called %d times, want 1 (no retry)", a.calls.Load())
	}
}

func TestFallbackTimeoutWithPositiveStatusCheckAcceptsA(t *testing.T) {
	a := &checking{stub: &stub{name: "a", send: fails(errTimeout)}}
	a.status = func(_ context.Context, key string, ch domain.Channel) (string, bool, error) {
		if key != "k1" || ch != domain.ChannelSMS {
			t.Errorf("status asked for %q %q", key, ch)
		}
		return "a-7", true, nil
	}
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(fallbackConfig(), Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "a" || out.messageID != "a-7" {
		t.Errorf("outcome = %+v", out)
	}
	if b.calls.Load() != 0 {
		t.Error("B was called although A had taken the message")
	}
}

func TestFallbackTimeoutWithNegativeOrFailedStatusCheckSwitchesToB(t *testing.T) {
	cases := map[string]func(context.Context, string, domain.Channel) (string, bool, error){
		"not found": func(context.Context, string, domain.Channel) (string, bool, error) { return "", false, nil },
		"error":     func(context.Context, string, domain.Channel) (string, bool, error) { return "", false, errDown },
	}
	for name, status := range cases {
		t.Run(name, func(t *testing.T) {
			a := &checking{stub: &stub{name: "a", send: fails(errTimeout)}, status: status}
			b := &stub{name: "b", send: accepts("b-1")}
			out := deliver(fallbackConfig(), Pair{A: a, B: b})
			if out.state != domain.StateAccepted || out.provider != "b" {
				t.Errorf("outcome = %+v", out)
			}
			if a.checks.Load() != 1 {
				t.Errorf("status checked %d times, want 1", a.checks.Load())
			}
		})
	}
}

func TestFallbackTimeoutWithNoStatusCheckSwitchesToB(t *testing.T) {
	a, b := &stub{name: "a", send: fails(errTimeout)}, &stub{name: "b", send: accepts("b-1")}
	out := deliver(fallbackConfig(), Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "b" {
		t.Errorf("outcome = %+v", out)
	}
}

func TestFallbackStatusCheckHasItsOwnTimeout(t *testing.T) {
	a := &checking{stub: &stub{name: "a", send: fails(errTimeout)}}
	a.status = func(ctx context.Context, _ string, _ domain.Channel) (string, bool, error) {
		<-ctx.Done()
		return "", false, ctx.Err()
	}
	cfg := fallbackConfig()
	cfg.StatusTimeout = 20 * time.Millisecond
	b := &stub{name: "b", send: accepts("b-1")}
	start := time.Now()
	out := deliver(cfg, Pair{A: a, B: b})
	if out.provider != "b" || time.Since(start) > 400*time.Millisecond {
		t.Errorf("outcome = %+v after %v", out, time.Since(start))
	}
}

func TestFallbackOpenBreakerSwitchesWithoutStatusCheck(t *testing.T) {
	a := &checking{stub: &stub{name: "a", send: fails(ErrCircuitOpen)}}
	a.status = func(context.Context, string, domain.Channel) (string, bool, error) { return "x", true, nil }
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(fallbackConfig(), Pair{A: a, B: b})
	if out.provider != "b" || a.checks.Load() != 0 {
		t.Errorf("outcome = %+v, status checks %d", out, a.checks.Load())
	}
}

func TestFallbackNothingAcceptedOutcomes(t *testing.T) {
	cases := []struct {
		name string
		a, b error
		want domain.State
	}{
		{"both time out", errTimeout, errTimeout, domain.StateUnresolved},
		{"rejected then timeout", errRejected, errTimeout, domain.StateUnresolved},
		{"timeout then rejected", errTimeout, errRejected, domain.StateUnresolved},
		{"both rejected", errRejected, errRejected, domain.StateFailed},
		{"both rate limited", errRateLimit, errRateLimit, domain.StateFailed},
		{"both down", errDown, ErrCircuitOpen, domain.StateFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := deliver(fallbackConfig(), Pair{A: &stub{name: "a", send: fails(c.a)}, B: &stub{name: "b", send: fails(c.b)}})
			if out.state != c.want {
				t.Errorf("state = %s, want %s", out.state, c.want)
			}
			if !strings.Contains(out.reason, "a: ") || !strings.Contains(out.reason, "b: ") {
				t.Errorf("reason = %q, want both providers", out.reason)
			}
		})
	}
}

func TestFallbackWithoutBHasNoFallback(t *testing.T) {
	out := deliver(fallbackConfig(), Pair{A: &stub{name: "a", send: fails(errDown)}})
	if out.state != domain.StateFailed {
		t.Errorf("outcome = %+v", out)
	}
	out = deliver(fallbackConfig(), Pair{A: &stub{name: "a", send: fails(errTimeout)}})
	if out.state != domain.StateUnresolved {
		t.Errorf("outcome = %+v", out)
	}
}

func TestFallbackNoProviderFails(t *testing.T) {
	out := deliver(fallbackConfig(), Pair{})
	if out.state != domain.StateFailed || !strings.Contains(out.reason, "no provider") {
		t.Errorf("outcome = %+v", out)
	}
}

func TestFallbackProviderTimeoutIsPerProvider(t *testing.T) {
	cfg := fallbackConfig()
	cfg.ProviderATimeout = 20 * time.Millisecond
	a := &stub{name: "a", send: func(ctx context.Context, _ domain.Notification) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}}
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(cfg, Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "b" {
		t.Errorf("outcome = %+v", out)
	}
}

func TestFallbackOverallDeadlineLeavesNoTimeForB(t *testing.T) {
	cfg := fallbackConfig()
	cfg.SendTimeout = 30 * time.Millisecond
	a := &stub{name: "a", send: func(ctx context.Context, _ domain.Notification) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}}
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(cfg, Pair{A: a, B: b})
	if out.state != domain.StateUnresolved || b.calls.Load() != 0 {
		t.Errorf("outcome = %+v, B calls %d", out, b.calls.Load())
	}
}

func TestFallbackRejectedThenNoTimeIsFailed(t *testing.T) {
	cfg := fallbackConfig()
	cfg.SendTimeout = 30 * time.Millisecond
	a := &stub{name: "a", send: func(ctx context.Context, _ domain.Notification) (string, error) {
		<-ctx.Done() // use up the whole deadline, then refuse
		return "", errRejected
	}}
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(cfg, Pair{A: a, B: b})
	if out.state != domain.StateFailed || b.calls.Load() != 0 {
		t.Errorf("outcome = %+v, B calls %d", out, b.calls.Load())
	}
}

func TestFallbackStatusCheckRunsAfterTheSendDeadline(t *testing.T) {
	cfg := fallbackConfig()
	cfg.SendTimeout = 30 * time.Millisecond
	a := &checking{stub: &stub{name: "a", send: func(ctx context.Context, _ domain.Notification) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}}}
	a.status = func(context.Context, string, domain.Channel) (string, bool, error) { return "a-7", true, nil }
	out := deliver(cfg, Pair{A: a, B: &stub{name: "b", send: accepts("b-1")}})
	if out.state != domain.StateAccepted || out.messageID != "a-7" {
		t.Errorf("outcome = %+v", out)
	}
}

func TestFallbackRateLimitWaitIsNotATimeout(t *testing.T) {
	a := WithRateLimit(&stub{name: "a", send: accepts("a-1")}, 1)
	_, _ = a.Send(context.Background(), domain.Notification{}) // use the burst
	cfg := fallbackConfig()
	cfg.ProviderATimeout = 10 * time.Millisecond
	b := &stub{name: "b", send: accepts("b-1")}
	out := deliver(cfg, Pair{A: a, B: b})
	if out.state != domain.StateAccepted || out.provider != "b" {
		t.Errorf("outcome = %+v", out)
	}
}
