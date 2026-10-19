package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// recordTimeout bounds an outcome write, which must not share the send's
// deadline: a send that used its whole timeout still has to be recorded.
const recordTimeout = 5 * time.Second

// DelivererConfig tunes a Deliverer.
type DelivererConfig struct {
	SendTimeout   time.Duration // limit for a whole channel send, across all its providers
	StatusTimeout time.Duration // limit for the status check after a provider times out
}

// Deliverer sends one notification through its channel's provider chain and
// records the outcome. The sender uses it for a row's first send and the
// janitor for a re-drive, so both follow the same fallback rules.
type Deliverer struct {
	store  ports.NotificationStore
	chains map[domain.Channel][]Route
	cfg    DelivererConfig
	log    *slog.Logger
}

// NewDeliverer returns a Deliverer. chains maps each channel to its ordered
// fallback chain; a channel without one has its rows failed.
func NewDeliverer(store ports.NotificationStore, chains map[domain.Channel][]Route, cfg DelivererConfig, log *slog.Logger) (*Deliverer, error) {
	if cfg.SendTimeout <= 0 || cfg.StatusTimeout <= 0 {
		return nil, errors.New("sender: send and status timeouts must be positive")
	}
	for c, chain := range chains {
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
	return &Deliverer{store: store, chains: chains, cfg: cfg, log: log}, nil
}

// Send runs the fallback flow from the first provider and records its outcome.
// It does not stop when ctx is cancelled, so a shutdown does not leave a sent
// notification unrecorded. It does not recover a provider's panic; the caller
// must, so one bad adapter cannot take the process down.
func (d *Deliverer) Send(ctx context.Context, n domain.Notification) {
	base, log := d.start(ctx, n)
	d.finish(base, log, n, d.deliver(base, log, n, d.chains[n.Channel], 0))
}

// Resume re-drives a pending row from the last provider the earlier send
// called (n.LastProvider; the first provider if it is empty or no longer in
// the chain). That provider is asked first, by idempotency key, whether it
// already took the message, which covers a crash after the send and before the
// outcome was written: if it did, the row is accepted with its message id and
// nothing is sent. Otherwise the message is sent through it and, on failure,
// through the providers after it.
//
// The providers before the resume point come last, after the later ones, so a
// provider the sender skipped (open breaker, no time left) is still tried and
// failed only means every provider refused the message. If the resume
// provider cannot answer a status check, the message is sent through it again
// and may be delivered twice, as after any timeout.
//
// Like Send, it does not recover a provider's panic; the caller must.
func (d *Deliverer) Resume(ctx context.Context, n domain.Notification) {
	base, log := d.start(ctx, n)
	chain := d.chains[n.Channel]
	first := resumeIndex(chain, n.LastProvider)
	if n.LastProvider != nil && *n.LastProvider != "" && len(chain) > 0 && chain[first].Provider.Name() != *n.LastProvider {
		log.WarnContext(base, "last provider is not in the chain; resuming from the first", "last_provider", *n.LastProvider)
	}
	if first < len(chain) {
		if id, found := d.checkStatus(base, log, n, chain[first].Provider); found {
			d.finish(base, log, n, outcome{state: domain.StateAccepted, provider: chain[first].Provider.Name(), messageID: id})
			return
		}
	}
	d.finish(base, log, n, d.deliver(base, log, n, chain, first))
}

// resumeIndex is the position in chain of the provider named last, or 0 if
// last is nil, empty or not in the chain.
func resumeIndex(chain []Route, last *string) int {
	if last == nil {
		return 0
	}
	for i, r := range chain {
		if r.Provider.Name() == *last {
			return i
		}
	}
	return 0
}

// start detaches the send from ctx's cancellation and builds its logger.
func (d *Deliverer) start(ctx context.Context, n domain.Notification) (context.Context, *slog.Logger) {
	return context.WithoutCancel(ctx),
		d.log.With("notification_id", n.ID, "idempotency_key", n.IdempotencyKey, "channel", n.Channel)
}

// finish writes the outcome of a send. A pending outcome has nothing to write
// and only notes the last provider called.
func (d *Deliverer) finish(base context.Context, log *slog.Logger, n domain.Notification, out outcome) {
	var write func(context.Context) error
	switch out.state {
	case domain.StateAccepted:
		write = func(ctx context.Context) error { return d.store.MarkAccepted(ctx, n.ID, out.provider, out.messageID) }
	case domain.StateUnresolved:
		write = func(ctx context.Context) error { return d.store.MarkUnresolved(ctx, n.ID, failureReason(out.reason)) }
	case domain.StateFailed:
		write = func(ctx context.Context) error { return d.store.MarkFailed(ctx, n.ID, failureReason(out.reason)) }
	default:
		// Pending: some provider was never tried, so there is no outcome and the
		// row stays pending for the janitor, which resumes from the last provider
		// called. Any other state is a bug and is treated the same way, never as
		// a failure.
		log.WarnContext(base, "no outcome to record; row stays pending for the janitor",
			"state", out.state, "last_provider", out.lastTried, "reason", out.reason)
		if out.lastTried != "" {
			d.record(base, log, func(ctx context.Context) error {
				return d.store.RecordLastProvider(ctx, n.ID, out.lastTried)
			})
		}
		return
	}
	d.record(base, log, write)
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
func (d *Deliverer) record(base context.Context, log *slog.Logger, write func(context.Context) error) {
	ctx, cancel := context.WithTimeout(base, recordTimeout)
	defer cancel()
	if err := write(ctx); err != nil {
		log.ErrorContext(ctx, "record outcome; row stays pending", "error", err)
	}
}
