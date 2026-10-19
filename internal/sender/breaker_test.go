package sender

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type scripted struct {
	errs  []error // consumed one per call; nil means success
	calls int
}

func (s *scripted) Name() string { return "scripted" }
func (s *scripted) Send(context.Context, domain.Notification) (string, error) {
	i := s.calls
	s.calls++
	if i < len(s.errs) {
		return "", s.errs[i]
	}
	return "id", nil
}

func newBreaker(p ports.Provider, c *clock) ports.Provider {
	return WithBreaker(p, BreakerConfig{FailureThreshold: 3, OpenFor: time.Minute, Now: c.now})
}

func send(p ports.Provider) error {
	_, err := p.Send(context.Background(), domain.Notification{})
	return err
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	c := &clock{}
	fail := fmt.Errorf("%w: down", ports.ErrTransient)
	p := &scripted{errs: []error{fail, fail, ports.ErrTimeout}}
	b := newBreaker(p, c)

	for i := range 3 {
		if err := send(b); err == nil || errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d err = %v, want the provider's error", i, err)
		}
	}
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	if p.calls != 3 {
		t.Errorf("provider called %d times, want 3", p.calls)
	}
}

func TestBreakerSuccessResetsTheCount(t *testing.T) {
	c := &clock{}
	fail := ports.ErrTransient
	p := &scripted{errs: []error{fail, fail, nil, fail, fail}}
	b := newBreaker(p, c)
	for range 5 {
		_ = send(b)
	}
	if err := send(b); errors.Is(err, ErrCircuitOpen) {
		t.Fatal("breaker opened although failures were not consecutive")
	}
}

func TestBreakerIgnoresRateLimitAndRejection(t *testing.T) {
	c := &clock{}
	var errs []error
	for range 10 {
		errs = append(errs, ports.ErrRateLimited, ports.ErrRejected)
	}
	p := &scripted{errs: errs}
	b := newBreaker(p, c)
	for range 20 {
		if err := send(b); errors.Is(err, ErrCircuitOpen) {
			t.Fatal("breaker opened on a rate limit or rejection")
		}
	}
	if p.calls != 20 {
		t.Errorf("provider called %d times, want 20", p.calls)
	}
}

func TestBreakerProbeClosesOnSuccess(t *testing.T) {
	c := &clock{}
	p := &scripted{errs: []error{ports.ErrTimeout, ports.ErrTimeout, ports.ErrTimeout}}
	b := newBreaker(p, c)
	for range 3 {
		_ = send(b)
	}
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want open", err)
	}

	c.advance(time.Minute)
	if err := send(b); err != nil { // the probe succeeds
		t.Fatalf("probe err = %v", err)
	}
	for range 5 {
		if err := send(b); err != nil {
			t.Fatalf("closed breaker err = %v", err)
		}
	}
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	c := &clock{}
	p := &scripted{errs: []error{ports.ErrTimeout, ports.ErrTimeout, ports.ErrTimeout, ports.ErrTimeout}}
	b := newBreaker(p, c)
	for range 3 {
		_ = send(b)
	}
	c.advance(time.Minute)
	if err := send(b); !errors.Is(err, ports.ErrTimeout) { // the probe fails
		t.Fatalf("probe err = %v", err)
	}
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want open again", err)
	}
	c.advance(30 * time.Second)
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want still open before OpenFor", err)
	}
	c.advance(30 * time.Second)
	if err := send(b); err != nil {
		t.Fatalf("second probe err = %v", err)
	}
}

func TestBreakerAllowsOneProbeAtATime(t *testing.T) {
	c := &clock{}
	release := make(chan struct{})
	started := make(chan struct{})
	p := &blocking{release: release, started: started}
	b := newBreaker(p, c)
	p.fail.Store(true)
	for range 3 {
		_ = send(b)
	}
	p.fail.Store(false)
	c.advance(time.Minute)

	done := make(chan error, 1)
	go func() { done <- send(b) }()
	<-started // the probe is in flight
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second call err = %v, want open while probing", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("probe err = %v", err)
	}
}

type blocking struct {
	fail    atomic.Bool
	release chan struct{}
	started chan struct{}
}

func (p *blocking) Name() string { return "blocking" }
func (p *blocking) Send(context.Context, domain.Notification) (string, error) {
	if p.fail.Load() {
		return "", ports.ErrTimeout
	}
	p.started <- struct{}{}
	<-p.release
	return "id", nil
}

func TestBreakerKeepsStatusCheck(t *testing.T) {
	var p ports.Provider = &statusful{}
	for name, wrapped := range map[string]ports.Provider{
		"breaker":     newBreaker(p, &clock{}),
		"rate limit":  WithRateLimit(p, 100),
		"both":        WithRateLimit(newBreaker(p, &clock{}), 100),
		"not checker": newBreaker(&scripted{}, &clock{}),
	} {
		_, ok := wrapped.(ports.StatusChecker)
		if want := name != "not checker"; ok != want {
			t.Errorf("%s: StatusChecker = %v, want %v", name, ok, want)
		}
	}
}

type statusful struct{ scripted }

func (*statusful) Status(context.Context, string, domain.Channel) (string, bool, error) {
	return "", false, nil
}

// gated blocks the call for key "slow" until released and fails every other.
type gated struct {
	release chan struct{}
	started chan struct{}
}

func (g *gated) Name() string { return "gated" }
func (g *gated) Send(_ context.Context, n domain.Notification) (string, error) {
	if n.IdempotencyKey != "slow" {
		return "", ports.ErrTimeout
	}
	g.started <- struct{}{}
	<-g.release
	return "id", nil
}

func TestBreakerIgnoresCallsStartedBeforeItOpened(t *testing.T) {
	g := &gated{release: make(chan struct{}), started: make(chan struct{})}
	b := WithBreaker(g, BreakerConfig{FailureThreshold: 1, OpenFor: time.Minute, Now: (&clock{}).now})

	done := make(chan error, 1)
	go func() {
		_, err := b.Send(context.Background(), domain.Notification{IdempotencyKey: "slow"})
		done <- err
	}()
	<-g.started // in flight while the breaker is still closed

	_, _ = b.Send(context.Background(), domain.Notification{IdempotencyKey: "fast"}) // opens it
	close(g.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := send(b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want a late success not to close the breaker", err)
	}
}

func TestBreakerCancelledCallIsNotAFailure(t *testing.T) {
	p := &scripted{errs: []error{context.Canceled, context.Canceled, context.Canceled, context.Canceled}}
	b := newBreaker(p, &clock{})
	for range 4 {
		if err := send(b); errors.Is(err, ErrCircuitOpen) {
			t.Fatal("breaker opened on cancelled calls")
		}
	}
}

func TestBreakerReleasesProbeOnPanic(t *testing.T) {
	c := &clock{}
	p := &scripted{errs: []error{ports.ErrTimeout, ports.ErrTimeout, ports.ErrTimeout}}
	b := newBreaker(p, c)
	for range 3 {
		_ = send(b)
	}
	c.advance(time.Minute)
	pp := &panicky{}
	br := b.(*breaker)
	br.Provider = pp
	func() {
		defer func() { _ = recover() }()
		_ = send(b)
	}()
	c.advance(time.Minute)
	br.Provider = &scripted{}
	if err := send(b); err != nil {
		t.Fatalf("err = %v, want a new probe after the panic", err)
	}
}

type panicky struct{}

func (panicky) Name() string { return "panicky" }
func (panicky) Send(context.Context, domain.Notification) (string, error) {
	panic("boom")
}
