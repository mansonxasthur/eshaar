// Package mock simulates an external provider. It can accept, fail, delay,
// answer with a rate-limit response, take a message and lose the reply, answer
// status checks, and report a delivery or a failure by webhook after accepting.
package mock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Step scripts one Send call.
type Step struct {
	// Delay is how long the call takes, or until its context ends.
	Delay time.Duration

	// Err, if set, is returned after the delay.
	Err error

	// Took makes the provider take the message on arrival, before the delay, so
	// a call that times out or fails still leaves the message with the provider.
	// A status check finds it.
	Took bool
}

// Event is a webhook the provider reports after it accepted a message. Status
// is domain.StateDelivered or domain.StateFailed.
type Event struct {
	Provider       string
	EventID        string // unique per event of one provider, so a resent event is a duplicate
	MessageID      string
	IdempotencyKey string
	Channel        domain.Channel
	Status         domain.State
	Reason         string
}

// JSON returns the body a real provider would POST to the webhook endpoint for
// this event: the fields the endpoint reads, as JSON. The caller signs it with
// the provider's secret. The mock does not import the webhook package, so this
// shape is the contract between the two, and the end-to-end test of cmd/webhook
// holds them together.
func (e Event) JSON() []byte {
	b, err := json.Marshal(map[string]string{
		"event_id":        e.EventID,
		"message_id":      e.MessageID,
		"idempotency_key": e.IdempotencyKey,
		"channel":         string(e.Channel),
		"status":          string(e.Status),
		"reason":          e.Reason,
	})
	if err != nil { // a map of strings always marshals
		panic(err)
	}
	return b
}

// Provider accepts notifications and remembers them. Message and event ids carry
// a random part chosen when the Provider is built: they must stay unique across
// restarts, since the database keeps one row per (provider, message id) and one
// event per (provider, event id).
type Provider struct {
	name string
	run  string // random per Provider, so ids do not repeat after a restart

	delay     time.Duration
	err       error
	errorRate float64 // share of default-behavior sends that fail with ports.ErrTransient
	script    []Step
	statusErr error

	lateAfter  time.Duration
	lateStatus domain.State
	lateReason string
	lateNotify func(Event)

	calls    atomic.Int64
	seq      atomic.Int64
	eventSeq atomic.Int64

	mu    sync.Mutex
	sent  []domain.Notification
	taken map[string]string // idempotency key + channel -> message ID
}

var (
	_ ports.Provider      = (*Provider)(nil)
	_ ports.StatusChecker = (*Provider)(nil)
)

// Option changes how a Provider behaves. Options that set the default send
// behavior (WithDelay, WithError, WithRateLimited) override each other; the
// last one wins.
type Option func(*Provider)

// WithDelay makes every send take d, or until its context ends.
func WithDelay(d time.Duration) Option { return func(p *Provider) { p.delay = d } }

// WithError makes every send fail with err.
func WithError(err error) Option { return func(p *Provider) { p.err = err } }

// WithErrorRate makes a share of the sends (0 to 1) fail with a transient
// error, chosen at random per call; the rest follow the other options. It
// models a provider that is partly down, for the load test. It applies only
// when WithError is not set, and scripted steps take precedence over it.
func WithErrorRate(rate float64) Option { return func(p *Provider) { p.errorRate = rate } }

// WithRateLimited makes every send answer with a rate-limit response.
func WithRateLimited() Option {
	return func(p *Provider) { p.err = fmt.Errorf("mock %s: %w", p.name, ports.ErrRateLimited) }
}

// WithScript sets the behavior of the first calls, one step each. Later calls
// use the provider's default behavior.
func WithScript(steps ...Step) Option { return func(p *Provider) { p.script = steps } }

// WithStatusError makes every status check fail with err.
func WithStatusError(err error) Option { return func(p *Provider) { p.statusErr = err } }

// WithLateFailure makes the provider accept a message and, after d, call notify
// with a failed event for it. The webhook receiver consumes these in tests.
func WithLateFailure(d time.Duration, notify func(Event)) Option {
	return withLateReport(d, domain.StateFailed, "mock late failure", notify)
}

// WithLateDelivery makes the provider accept a message and, after d, call notify
// with a delivered event for it. With a d of zero the event can reach the
// receiver before the sender has recorded the accept, which is how a test
// exercises an event that beats its notification.
func WithLateDelivery(d time.Duration, notify func(Event)) Option {
	return withLateReport(d, domain.StateDelivered, "", notify)
}

func withLateReport(d time.Duration, status domain.State, reason string, notify func(Event)) Option {
	return func(p *Provider) {
		p.lateAfter, p.lateStatus, p.lateReason, p.lateNotify = d, status, reason, notify
	}
}

// NewProvider returns a provider called name.
func NewProvider(name string, opts ...Option) *Provider {
	var run [4]byte
	if _, err := rand.Read(run[:]); err != nil { // never fails on supported platforms
		panic(err)
	}
	p := &Provider{name: name, run: hex.EncodeToString(run[:]), taken: map[string]string{}}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Name implements ports.Provider.
func (p *Provider) Name() string { return p.name }

// Send implements ports.Provider. It returns a new message ID per taken
// message. A delay that outlasts the context's deadline returns ports.ErrTimeout.
func (p *Provider) Send(ctx context.Context, n domain.Notification) (string, error) {
	st := p.nextStep()
	var id string
	if st.Took {
		id = p.take(n)
	}
	if err := wait(ctx, st.Delay); err != nil {
		return "", err
	}
	if st.Err != nil {
		return "", st.Err
	}
	if id == "" {
		id = p.take(n)
	}
	p.scheduleReport(n, id)
	return id, nil
}

// Status implements ports.StatusChecker. It finds the messages the provider
// took, including ones whose send call failed or timed out.
func (p *Provider) Status(ctx context.Context, idempotencyKey string, channel domain.Channel) (string, bool, error) {
	if err := wait(ctx, 0); err != nil {
		return "", false, err
	}
	if p.statusErr != nil {
		return "", false, p.statusErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id, ok := p.taken[takenKey(idempotencyKey, channel)]
	return id, ok, nil
}

// Sent returns the notifications the provider took, in order.
func (p *Provider) Sent() []domain.Notification {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Notification(nil), p.sent...)
}

func (p *Provider) nextStep() Step {
	if i := int(p.calls.Add(1)) - 1; i < len(p.script) {
		return p.script[i]
	}
	err := p.err
	if err == nil && p.errorRate > 0 && mrand.Float64() < p.errorRate {
		err = fmt.Errorf("mock %s: %w", p.name, ports.ErrTransient)
	}
	return Step{Delay: p.delay, Err: err}
}

func (p *Provider) take(n domain.Notification) string {
	p.mu.Lock()
	id := fmt.Sprintf("%s-%s-%d", p.name, p.run, p.seq.Add(1))
	p.sent = append(p.sent, n)
	p.taken[takenKey(n.IdempotencyKey, n.Channel)] = id
	p.mu.Unlock()
	return id
}

func (p *Provider) scheduleReport(n domain.Notification, id string) {
	if p.lateNotify == nil {
		return
	}
	ev := Event{
		Provider:       p.name,
		EventID:        fmt.Sprintf("%s-%s-evt-%d", p.name, p.run, p.eventSeq.Add(1)),
		MessageID:      id,
		IdempotencyKey: n.IdempotencyKey,
		Channel:        n.Channel,
		Status:         p.lateStatus,
		Reason:         p.lateReason,
	}
	time.AfterFunc(p.lateAfter, func() { p.lateNotify(ev) })
}

func takenKey(idempotencyKey string, channel domain.Channel) string {
	return idempotencyKey + "|" + string(channel)
}

// wait sleeps for d, or until ctx ends. A deadline is reported as a provider
// timeout, a cancellation as the context's own error.
func wait(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return ctxError(err)
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctxError(ctx.Err())
	}
}

func ctxError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ports.ErrTimeout, err)
	}
	return err
}

// WithoutStatusCheck hides p's status check, so it behaves like a provider that
// cannot report on a message by idempotency key.
func WithoutStatusCheck(p ports.Provider) ports.Provider {
	return struct{ ports.Provider }{p}
}
