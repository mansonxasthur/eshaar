// Package mock simulates an external provider. Phase 4 extends it with
// failures, rate limits and late webhooks; for now it accepts every send.
package mock

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Provider accepts notifications and remembers them.
type Provider struct {
	name  string
	delay time.Duration
	err   error
	seq   atomic.Int64

	mu   sync.Mutex
	sent []domain.Notification
}

var _ ports.Provider = (*Provider)(nil)

// Option changes how a Provider behaves.
type Option func(*Provider)

// WithDelay makes every send take d, or until its context ends.
func WithDelay(d time.Duration) Option { return func(p *Provider) { p.delay = d } }

// WithError makes every send fail with err.
func WithError(err error) Option { return func(p *Provider) { p.err = err } }

// NewProvider returns a provider called name.
func NewProvider(name string, opts ...Option) *Provider {
	p := &Provider{name: name}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Name implements ports.Provider.
func (p *Provider) Name() string { return p.name }

// Send implements ports.Provider. It returns a new message ID per accepted send.
func (p *Provider) Send(ctx context.Context, n domain.Notification) (string, error) {
	if p.delay > 0 {
		t := time.NewTimer(p.delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if p.err != nil {
		return "", p.err
	}
	p.mu.Lock()
	p.sent = append(p.sent, n)
	p.mu.Unlock()
	return fmt.Sprintf("%s-%d", p.name, p.seq.Add(1)), nil
}

// Sent returns the notifications accepted so far, in order.
func (p *Provider) Sent() []domain.Notification {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Notification(nil), p.sent...)
}
