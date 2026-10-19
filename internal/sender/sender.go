// Package sender is the service behind cmd/sender: it saves each consumed
// batch, acknowledges it, then sends every channel through a bounded pool.
package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Config tunes the sender.
type Config struct {
	PoolSize      int           // concurrent channel sends; a full pool stops the consume loop
	SendTimeout   time.Duration // limit for a whole channel send, across all its providers
	StatusTimeout time.Duration // limit for the status check after a provider times out
	DrainTimeout  time.Duration // how long Run waits for in-flight sends on shutdown
	MinBackoff    time.Duration // first wait after a failed poll or insert
	MaxBackoff    time.Duration // cap on that wait
}

// recordTimeout bounds an outcome write, which must not share the send's
// deadline: a send that used its whole timeout still has to be recorded.
const recordTimeout = 5 * time.Second

// Sender consumes batches, saves them, acknowledges, then sends.
type Sender struct {
	consumer  ports.Consumer
	store     ports.NotificationStore
	providers map[domain.Channel][]Route
	cfg       Config
	log       *slog.Logger
	now       func() time.Time

	slots chan struct{}
	wg    sync.WaitGroup
}

// New returns a Sender. providers maps each channel to its ordered fallback
// chain; a channel without one has its rows failed.
func New(consumer ports.Consumer, store ports.NotificationStore, providers map[domain.Channel][]Route, cfg Config, log *slog.Logger) (*Sender, error) {
	if cfg.PoolSize < 1 {
		return nil, errors.New("sender: pool size must be at least 1")
	}
	if cfg.SendTimeout <= 0 || cfg.StatusTimeout <= 0 {
		return nil, errors.New("sender: send and status timeouts must be positive")
	}
	if cfg.MinBackoff <= 0 || cfg.MaxBackoff < cfg.MinBackoff {
		return nil, errors.New("sender: backoff needs 0 < min <= max")
	}
	for c, chain := range providers {
		var calls time.Duration
		for i, r := range chain {
			if r.Provider == nil || r.Timeout <= 0 {
				return nil, fmt.Errorf("sender: channel %s provider %d needs a provider and a positive timeout", c, i+1)
			}
			calls += r.Timeout
		}
		if len(chain) > 1 {
			calls += time.Duration(len(chain)-1) * cfg.StatusTimeout
		}
		if cfg.SendTimeout < calls {
			log.Warn("send timeout is shorter than every provider call plus status checks; a slow provider can leave the next no time",
				"channel", c, "send_timeout", cfg.SendTimeout, "needed", calls)
		}
	}
	return &Sender{
		consumer:  consumer,
		store:     store,
		providers: providers,
		cfg:       cfg,
		log:       log,
		now:       time.Now,
		slots:     make(chan struct{}, cfg.PoolSize),
	}, nil
}

// Run consumes until ctx is done, then waits for in-flight sends and returns
// nil. Rows whose send was cut off stay pending for the janitor.
func (s *Sender) Run(ctx context.Context) error {
	defer s.drain()
	backoff := s.cfg.MinBackoff
	for ctx.Err() == nil {
		batch, err := s.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.log.ErrorContext(ctx, "poll", "error", err)
			backoff = s.wait(ctx, backoff)
			continue
		}
		if err := s.handle(ctx, batch); err != nil {
			s.log.ErrorContext(ctx, "save batch", "error", err)
			backoff = s.wait(ctx, backoff)
			continue
		}
		backoff = s.cfg.MinBackoff
	}
	return nil
}

// handle saves a batch, acknowledges it and starts the sends. On a save error
// nothing is acknowledged: the batch is released so the queue redelivers it.
func (s *Sender) handle(ctx context.Context, batch ports.Batch) error {
	now := s.now()
	var rows []domain.Notification
	for _, req := range batch.Requests() {
		rows = append(rows, req.Notifications(now)...)
	}

	var inserted []domain.Notification
	if len(rows) > 0 {
		var err error
		if inserted, err = s.store.InsertPending(ctx, rows); err != nil {
			batch.Release()
			return fmt.Errorf("insert %d rows: %w", len(rows), err)
		}
	}
	if err := batch.Ack(ctx); err != nil {
		// The rows are saved and this process owns their sends. The redelivered
		// batch finds them already saved and will not send them, so send now.
		s.log.ErrorContext(ctx, "ack batch", "error", err)
	}
	s.dispatch(ctx, inserted)
	return nil
}

// dispatch starts one send per row, blocking while the pool is full. Blocking
// here is what stops the consume loop from reading, so the backlog waits in
// the queue and not in memory.
func (s *Sender) dispatch(ctx context.Context, rows []domain.Notification) {
	for i, n := range rows {
		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			s.log.WarnContext(ctx, "shutdown with unsent rows; they stay pending", "count", len(rows)-i)
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.slots }()
			// A provider that panics must not take the process, and every other
			// in-flight send, down. The row stays pending for the janitor.
			defer func() {
				if r := recover(); r != nil {
					s.log.ErrorContext(ctx, "send panicked; row stays pending", "notification_id", n.ID, "channel", n.Channel, "panic", fmt.Sprint(r))
				}
			}()
			s.send(ctx, n)
		}()
	}
}

// send runs the fallback flow and records its outcome. It does not stop when
// ctx is cancelled, so a shutdown does not leave a sent notification unrecorded.
func (s *Sender) send(ctx context.Context, n domain.Notification) {
	base := context.WithoutCancel(ctx)
	log := s.log.With("notification_id", n.ID, "idempotency_key", n.IdempotencyKey, "channel", n.Channel)

	out := s.deliver(base, log, n, s.providers[n.Channel])
	var write func(context.Context) error
	switch out.state {
	case domain.StateAccepted:
		write = func(ctx context.Context) error { return s.store.MarkAccepted(ctx, n.ID, out.provider, out.messageID) }
	case domain.StateUnresolved:
		write = func(ctx context.Context) error { return s.store.MarkUnresolved(ctx, n.ID, failureReason(out.reason)) }
	case domain.StateFailed:
		write = func(ctx context.Context) error { return s.store.MarkFailed(ctx, n.ID, failureReason(out.reason)) }
	default:
		// Pending: no provider was tried, so there is nothing to record and
		// the row stays pending for the janitor. Any other state is a bug and
		// is treated the same way, never as a failure.
		log.WarnContext(ctx, "no outcome to record; row stays pending for the janitor", "state", out.state, "reason", out.reason)
		return
	}
	s.record(base, log, write)
}

// maxReasonLen caps the stored failure reason.
const maxReasonLen = 500

// failureReason is the text stored in failure_reason. Providers must keep
// contact details out of their errors; the cap bounds what a verbose one adds.
func failureReason(text string) string {
	r := []rune(text)
	if len(r) > maxReasonLen {
		r = r[:maxReasonLen]
	}
	return string(r)
}

// record runs a store write. A failure is logged and the row stays pending for
// the janitor to re-drive.
func (s *Sender) record(base context.Context, log *slog.Logger, write func(context.Context) error) {
	ctx, cancel := context.WithTimeout(base, recordTimeout)
	defer cancel()
	if err := write(ctx); err != nil {
		log.ErrorContext(ctx, "record outcome; row stays pending", "error", err)
	}
}

// wait sleeps for d (or until ctx is done) and returns the next backoff.
func (s *Sender) wait(ctx context.Context, d time.Duration) time.Duration {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
	return min(d*2, s.cfg.MaxBackoff)
}

// drain waits for in-flight sends, up to DrainTimeout.
func (s *Sender) drain() {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(s.cfg.DrainTimeout):
		s.log.Warn("drain timed out; in-flight rows stay pending")
	}
}
