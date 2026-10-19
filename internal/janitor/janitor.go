// Package janitor is the service behind cmd/janitor: a recovery loop that
// re-drives pending rows nobody finished, and a sweeper that gives up on rows
// that used all their attempts.
package janitor

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

// Redriver sends a claimed row again, resuming its provider chain where the
// earlier send stopped and recording the outcome. *sender.Deliverer is one. It
// must not stop on cancellation, so a shutdown does not leave a sent
// notification unrecorded, and the janitor recovers its panics.
type Redriver interface {
	Resume(ctx context.Context, n domain.Notification)
}

// Config tunes the janitor.
type Config struct {
	Interval      time.Duration // time between recovery passes
	SweepInterval time.Duration // time between sweeper passes
	StaleAfter    time.Duration // age at which a pending row, or its last claim, counts as abandoned
	BatchSize     int           // most rows claimed (or swept) per statement
	Concurrency   int           // re-drives in flight at once
	RedriveBudget time.Duration // longest one re-drive can take (its status checks and send deadline)
}

// Janitor recovers stalled rows.
type Janitor struct {
	store   ports.JanitorStore
	redrive Redriver
	cfg     Config
	log     *slog.Logger
}

// New returns a Janitor. A recovery pass must finish within StaleAfter, or a
// second janitor could claim rows this one is still re-driving. New rejects a
// RedriveBudget longer than StaleAfter and warns when BatchSize rows at
// Concurrency, each taking RedriveBudget, do not fit in one StaleAfter.
func New(store ports.JanitorStore, redrive Redriver, cfg Config, log *slog.Logger) (*Janitor, error) {
	switch {
	case cfg.Interval <= 0 || cfg.SweepInterval <= 0 || cfg.StaleAfter <= 0 || cfg.RedriveBudget <= 0:
		return nil, errors.New("janitor: intervals, stale age and re-drive budget must be positive")
	case cfg.BatchSize < 1 || cfg.Concurrency < 1:
		return nil, errors.New("janitor: batch size and concurrency must be at least 1")
	}
	if cfg.RedriveBudget > cfg.StaleAfter {
		return nil, fmt.Errorf("janitor: a re-drive can take %s, longer than the stale age %s, so a row could be claimed again while it is being re-driven",
			cfg.RedriveBudget, cfg.StaleAfter)
	}
	waves := (cfg.BatchSize + cfg.Concurrency - 1) / cfg.Concurrency
	if worst := time.Duration(waves) * cfg.RedriveBudget; worst > cfg.StaleAfter {
		log.Warn("a full recovery pass can outlast the stale age; another janitor may re-claim rows still being re-driven",
			"batch_size", cfg.BatchSize, "concurrency", cfg.Concurrency, "worst_pass", worst, "stale_after", cfg.StaleAfter)
	}
	return &Janitor{store: store, redrive: redrive, cfg: cfg, log: log}, nil
}

// Run recovers and sweeps until ctx is done, then waits for the pass in
// progress and returns nil.
func (j *Janitor) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Go(func() { j.loop(ctx, j.cfg.Interval, j.recover) })
	wg.Go(func() { j.loop(ctx, j.cfg.SweepInterval, j.sweep) })
	wg.Wait()
	return nil
}

// loop runs pass now and then every interval. A pass runs to the end before
// the next starts, so one janitor never overlaps itself.
func (j *Janitor) loop(ctx context.Context, interval time.Duration, pass func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for ctx.Err() == nil {
		pass(ctx)
		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}
}

// recoverRow re-drives one row. A provider that panics must not take the
// process, and every other re-drive in flight, down. The row stays claimed and
// is claimed again after the stale age, until its attempts run out.
func (j *Janitor) recoverRow(ctx context.Context, n domain.Notification) {
	defer func() {
		if r := recover(); r != nil {
			j.log.ErrorContext(ctx, "re-drive panicked; row stays pending", "notification_id", n.ID, "channel", n.Channel, "panic", fmt.Sprint(r))
		}
	}()
	j.redrive.Resume(ctx, n)
}
