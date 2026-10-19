package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Provider is what the service knows about one webhook sender: the secret it
// signs with and the channels it serves. An event naming another channel is
// rejected, so a provider cannot report on a channel it does not serve.
type Provider struct {
	Secret   string
	Channels []domain.Channel
}

// Config tunes the service.
type Config struct {
	BatchSize       int           // events saved by one statement
	FlushInterval   time.Duration // longest an event waits for its batch to fill
	Flushers        int           // batches saved concurrently
	FlushTimeout    time.Duration // limit for saving one batch
	QueueSize       int           // events waiting for a flusher; a full queue holds requests back
	ResolveInterval time.Duration // how often events that beat their accept are re-resolved
	ResolveBatch    int           // events linked per pass
}

// maxResolvePasses bounds how many full passes one resolver tick may run, so a
// big backlog cannot keep it from noticing a shutdown.
const maxResolvePasses = 20

// ErrShuttingDown is returned to a request that arrives once the service has
// begun to stop. The endpoint answers 503, so the provider retries.
var ErrShuttingDown = errors.New("webhook service is shutting down")

// Service checks and parses webhooks, saves them in batches and links the ones
// that arrived before their notification was accepted.
type Service struct {
	store    ports.WebhookStore
	verifier *Verifier
	channels map[string]map[domain.Channel]bool
	cfg      Config
	log      *slog.Logger

	mu      sync.RWMutex // guards closed, and keeps a send on queue from racing its close
	closed  bool
	queue   chan *item
	started atomic.Bool

	refused, invalid throttle // log limits for unauthenticated and malformed events
}

var _ ports.EventReceiver = (*Service)(nil)

// item is one event waiting to be saved, and the way back to its request.
type item struct {
	event domain.WebhookEvent
	done  chan error // buffered, so a flusher never waits for a request that gave up
}

// New returns a Service. Every provider with a secret must serve at least one
// channel, and at least one provider must have a secret: with none, every
// webhook would be refused, which is a configuration mistake worth stopping at
// startup. A provider with an empty secret is left out.
func New(store ports.WebhookStore, providers map[string]Provider, cfg Config, log *slog.Logger) (*Service, error) {
	switch {
	case cfg.BatchSize < 1:
		return nil, errors.New("webhook: batch size must be at least 1")
	case cfg.Flushers < 1:
		return nil, errors.New("webhook: flushers must be at least 1")
	case cfg.QueueSize < 1:
		return nil, errors.New("webhook: queue size must be at least 1")
	case cfg.FlushInterval <= 0 || cfg.FlushTimeout <= 0 || cfg.ResolveInterval <= 0:
		return nil, errors.New("webhook: flush interval, flush timeout and resolve interval must be positive")
	case cfg.ResolveBatch < 1:
		return nil, errors.New("webhook: resolve batch must be at least 1")
	}
	secrets := make(map[string]string, len(providers))
	channels := make(map[string]map[domain.Channel]bool, len(providers))
	for name, p := range providers {
		if p.Secret == "" {
			continue
		}
		if len(p.Channels) == 0 {
			return nil, fmt.Errorf("webhook: provider %q has a secret but serves no channel", name)
		}
		secrets[name] = p.Secret
		channels[name] = make(map[domain.Channel]bool, len(p.Channels))
		for _, c := range p.Channels {
			channels[name][c] = true
		}
	}
	verifier, err := NewVerifier(secrets)
	if err != nil {
		return nil, fmt.Errorf("webhook: %w", err)
	}
	if verifier.Len() == 0 {
		return nil, errors.New("webhook: no provider has a webhook secret, so every webhook would be refused")
	}
	return &Service{
		store:    store,
		verifier: verifier,
		channels: channels,
		cfg:      cfg,
		log:      log,
		queue:    make(chan *item, cfg.QueueSize),
	}, nil
}

// Receive implements ports.EventReceiver. It returns nil once the event is
// saved. The wait covers the event's batch filling or timing out, then one
// statement, so the provider is only told 200 for events that are stored.
//
// If ctx ends while the event waits, Receive returns ctx's error but the event
// is still saved with its batch; the provider's retry is then skipped as a
// duplicate or applied as a no-op.
func (s *Service) Receive(ctx context.Context, provider string, body []byte, signature string) error {
	if err := s.verifier.Verify(provider, body, signature); err != nil {
		// The provider name is the caller's, so it is not logged here.
		if ok, skipped := s.refused.allow(time.Now()); ok {
			s.log.WarnContext(ctx, "webhook refused: bad signature or unknown provider; a rotated secret looks like this", "reason", err.Error(), "suppressed", skipped)
		}
		return err
	}
	ev, err := ParseEvent(provider, body)
	if err != nil {
		if ok, skipped := s.invalid.allow(time.Now()); ok {
			s.log.WarnContext(ctx, "webhook event invalid", "provider", provider, "error", err, "suppressed", skipped)
		}
		return err
	}
	if !s.channels[provider][ev.Channel] {
		s.log.WarnContext(ctx, "webhook event names a channel the provider does not serve", "provider", provider, "channel", ev.Channel)
		return invalid("channel is not one this provider serves")
	}

	it := &item{event: ev, done: make(chan error, 1)}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrShuttingDown
	}
	select {
	case s.queue <- it:
		s.mu.RUnlock()
	case <-ctx.Done():
		s.mu.RUnlock()
		return ctx.Err()
	}
	select {
	case err := <-it.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run saves queued events and re-resolves early ones until ctx is done, then
// refuses new events, saves what is still queued and returns. Call it once.
//
// Stop the HTTP server first and cancel ctx after: a request still waiting for
// queue space holds back the shutdown until it gets a slot or its own context
// ends.
func (s *Service) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("webhook: Run called twice")
	}

	var flushers sync.WaitGroup
	for i := 0; i < s.cfg.Flushers; i++ {
		flushers.Add(1)
		go func() {
			defer flushers.Done()
			s.flushLoop()
		}()
	}
	resolverDone := make(chan struct{})
	go func() {
		defer close(resolverDone)
		s.resolveLoop(ctx)
	}()

	<-ctx.Done()
	s.mu.Lock()
	s.closed = true
	close(s.queue)
	s.mu.Unlock()
	flushers.Wait()
	<-resolverDone
	return nil
}
