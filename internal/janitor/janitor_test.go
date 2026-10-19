package janitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

type fakeStore struct {
	mu        sync.Mutex
	claims    [][]domain.Notification // returned by successive ClaimStale calls; empty after
	claimErr  error
	claimArgs []time.Duration
	limits    []int
	sweeps    []int // returned by successive SweepExhausted calls; 0 after
	sweepErr  error
	sweepCall int
}

func (s *fakeStore) ClaimStale(_ context.Context, staleAfter time.Duration, limit int) ([]domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimArgs = append(s.claimArgs, staleAfter)
	s.limits = append(s.limits, limit)
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if len(s.claims) == 0 {
		return nil, nil
	}
	rows := s.claims[0]
	s.claims = s.claims[1:]
	return rows, nil
}

func (s *fakeStore) SweepExhausted(_ context.Context, _ time.Duration, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepCall++
	if s.sweepErr != nil {
		return 0, s.sweepErr
	}
	if len(s.sweeps) == 0 {
		return 0, nil
	}
	n := s.sweeps[0]
	s.sweeps = s.sweeps[1:]
	return min(n, limit), nil
}

func (s *fakeStore) sweepCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweepCall
}

type fakeRedriver struct {
	mu       sync.Mutex
	resumed  []string
	inFlight atomic.Int32
	peak     atomic.Int32
	hold     time.Duration
	panicOn  string
}

func (r *fakeRedriver) Resume(_ context.Context, n domain.Notification) {
	cur := r.inFlight.Add(1)
	defer r.inFlight.Add(-1)
	for {
		p := r.peak.Load()
		if cur <= p || r.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	time.Sleep(r.hold)
	r.mu.Lock()
	r.resumed = append(r.resumed, n.ID)
	r.mu.Unlock()
	if n.ID == r.panicOn {
		panic("adapter bug")
	}
}

func (r *fakeRedriver) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.resumed...)
}

func rows(n int) []domain.Notification {
	var out []domain.Notification
	for i := range n {
		out = append(out, domain.Notification{ID: fmt.Sprint("n", i), Channel: domain.ChannelSMS})
	}
	return out
}

func testConfig() Config {
	return Config{Interval: time.Millisecond, SweepInterval: time.Millisecond, StaleAfter: 10 * time.Second,
		BatchSize: 3, Concurrency: 3, RedriveBudget: time.Second}
}

func newJanitor(t *testing.T, store *fakeStore, rd *fakeRedriver, cfg Config) *Janitor {
	t.Helper()
	j, err := New(store, rd, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRecoverClaimsThenResumesEveryRow(t *testing.T) {
	store := &fakeStore{claims: [][]domain.Notification{rows(7)}}
	rd := &fakeRedriver{}
	j := newJanitor(t, store, rd, testConfig())

	j.recover(context.Background())

	if got := len(rd.ids()); got != 7 {
		t.Errorf("resumed %d rows, want 7", got)
	}
	if store.limits[0] != 3 || store.claimArgs[0] != 10*time.Second {
		t.Errorf("claimed with limit %d stale %v", store.limits[0], store.claimArgs[0])
	}
}

func TestRecoverBoundsConcurrency(t *testing.T) {
	store := &fakeStore{claims: [][]domain.Notification{rows(10)}}
	rd := &fakeRedriver{hold: 5 * time.Millisecond}
	j := newJanitor(t, store, rd, testConfig())

	j.recover(context.Background())

	if got := len(rd.ids()); got != 10 {
		t.Errorf("resumed %d rows, want 10", got)
	}
	if p := rd.peak.Load(); p > 3 || p < 2 {
		t.Errorf("peak concurrency %d, want 2..3", p)
	}
}

func TestRecoverSurvivesAPanickingRow(t *testing.T) {
	store := &fakeStore{claims: [][]domain.Notification{rows(5)}}
	rd := &fakeRedriver{panicOn: "n2"}
	j := newJanitor(t, store, rd, testConfig())

	j.recover(context.Background())

	if got := len(rd.ids()); got != 5 {
		t.Errorf("resumed %d rows, want all 5 despite the panic", got)
	}
}

func TestRecoverClaimErrorIsLoggedNotFatal(t *testing.T) {
	store := &fakeStore{claimErr: errors.New("db down")}
	rd := &fakeRedriver{}
	newJanitor(t, store, rd, testConfig()).recover(context.Background())
	if len(rd.ids()) != 0 {
		t.Error("re-drove rows after a failed claim")
	}
}

func TestRecoverFinishesClaimedRowsOnShutdown(t *testing.T) {
	store := &fakeStore{claims: [][]domain.Notification{rows(6)}}
	rd := &fakeRedriver{hold: 5 * time.Millisecond}
	cfg := testConfig()
	cfg.Concurrency, cfg.BatchSize = 1, 1
	j := newJanitor(t, store, rd, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(8 * time.Millisecond); cancel() }()
	j.recover(ctx)

	// Each claimed row already used an attempt, so none is dropped.
	if n := len(rd.ids()); n != 6 {
		t.Errorf("resumed %d rows, want all 6", n)
	}
}

func TestSweepRunsBatchesUntilShort(t *testing.T) {
	store := &fakeStore{sweeps: []int{3, 3, 1}}
	cfg := testConfig()
	newJanitor(t, store, &fakeRedriver{}, cfg).sweep(context.Background())
	if got := store.sweepCalls(); got != 3 {
		t.Errorf("sweep called %d times, want 3", got)
	}
}

func TestSweepStopsOnError(t *testing.T) {
	store := &fakeStore{sweepErr: errors.New("db down")}
	newJanitor(t, store, &fakeRedriver{}, testConfig()).sweep(context.Background())
	if got := store.sweepCalls(); got != 1 {
		t.Errorf("sweep called %d times after an error, want 1", got)
	}
}

func TestRunLoopsUntilCancelled(t *testing.T) {
	store := &fakeStore{claims: [][]domain.Notification{rows(2), rows(1)}}
	rd := &fakeRedriver{}
	j := newJanitor(t, store, rd, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.Run(ctx) }()

	eventually(t, "both claims re-driven", func() bool { return len(rd.ids()) == 3 })
	eventually(t, "sweeper ticks", func() bool { return store.sweepCalls() >= 2 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"interval":               func(c *Config) { c.Interval = 0 },
		"sweep":                  func(c *Config) { c.SweepInterval = 0 },
		"stale":                  func(c *Config) { c.StaleAfter = 0 },
		"budget":                 func(c *Config) { c.RedriveBudget = 0 },
		"slow budget":            func(c *Config) { c.RedriveBudget = c.StaleAfter + time.Second },
		"batch":                  func(c *Config) { c.BatchSize = 0 },
		"concurrency":            func(c *Config) { c.Concurrency = 0 },
		"batch over concurrency": func(c *Config) { c.BatchSize = c.Concurrency + 1 },
		"slow interval":          func(c *Config) { c.Interval = c.StaleAfter + time.Second },
	} {
		cfg := testConfig()
		mutate(&cfg)
		if _, err := New(&fakeStore{}, &fakeRedriver{}, cfg, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("%s: accepted an invalid config", name)
		}
	}
}
