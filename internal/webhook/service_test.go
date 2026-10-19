package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// fakeStore records what the service hands it.
type fakeStore struct {
	mu       sync.Mutex
	batches  [][]domain.WebhookEvent
	reject   func(domain.WebhookEvent) bool // a batch holding a rejected event fails
	gate     chan struct{}                  // when set, RecordEvents waits for it to close
	saveErr  error                          // fails every call when set
	resolves chan int                       // when set, ResolvePending takes its answers from it
	resolved atomic.Int64
	resolveE error
}

func (f *fakeStore) RecordEvents(ctx context.Context, events []domain.WebhookEvent) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		f.batches = append(f.batches, nil)
		return f.saveErr
	}
	if f.reject != nil {
		for _, e := range events {
			if f.reject(e) {
				f.batches = append(f.batches, nil)
				return errors.New("store refused the event")
			}
		}
	}
	f.batches = append(f.batches, append([]domain.WebhookEvent(nil), events...))
	return nil
}

func (f *fakeStore) ResolvePending(_ context.Context, _ int) (int, error) {
	f.resolved.Add(1)
	if f.resolveE != nil {
		return 0, f.resolveE
	}
	select {
	case n := <-f.resolves:
		return n, nil
	default:
		return 0, nil
	}
}

func (f *fakeStore) saved() (calls int, events []domain.WebhookEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.batches {
		events = append(events, b...)
	}
	return len(f.batches), events
}

var testProviders = map[string]Provider{
	"sms-a":   {Secret: secretA, Channels: []domain.Channel{domain.ChannelSMS}},
	"multi":   {Secret: "fedcba9876543210", Channels: []domain.Channel{domain.ChannelSMS, domain.ChannelWhatsApp}},
	"no-keys": {Channels: []domain.Channel{domain.ChannelEmail}},
}

func testConfig() Config {
	return Config{
		BatchSize: 100, FlushInterval: 5 * time.Millisecond, Flushers: 1, FlushTimeout: 2 * time.Second,
		QueueSize: 100, ResolveInterval: 5 * time.Millisecond, ResolveBatch: 10,
	}
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// start builds a service and runs it until the test ends.
func start(t *testing.T, store *fakeStore, cfg Config) *Service {
	t.Helper()
	s, err := New(store, testProviders, cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return")
		}
	})
	return s
}

func signedBody(id string) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"%s","message_id":"m-%s","idempotency_key":"k","channel":"sms","status":"delivered"}`, id, id))
}

func receive(s *Service, provider, secret string, body []byte) error {
	return s.Receive(context.Background(), provider, body, Sign(secret, body))
}

func TestReceiveSavesTheEventAndAnswersAfterwards(t *testing.T) {
	store := &fakeStore{gate: make(chan struct{})}
	s := start(t, store, testConfig())

	result := make(chan error, 1)
	go func() { result <- receive(s, "sms-a", secretA, signedBody("e1")) }()
	select {
	case err := <-result:
		t.Fatalf("answered before the event was saved: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.gate)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, events := store.saved(); len(events) != 1 || events[0].EventID != "e1" || events[0].Provider != "sms-a" || events[0].Status != domain.StateDelivered {
		t.Errorf("saved = %+v", events)
	}
}

func TestEventsShareABatch(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 5, time.Minute // only a full batch flushes
	s := start(t, store, cfg)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receive(s, "sms-a", secretA, signedBody(fmt.Sprint("e", i))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls, events := store.saved(); calls != 1 || len(events) != 5 {
		t.Errorf("%d calls with %d events, want 1 call with 5", calls, len(events))
	}
}

func TestAPartialBatchFlushesAfterTheInterval(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 100, 20*time.Millisecond
	s := start(t, store, cfg)
	began := time.Now()
	if err := receive(s, "sms-a", secretA, signedBody("e1")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(began); d < 15*time.Millisecond || d > 2*time.Second {
		t.Errorf("answered after %v, want about the 20ms interval", d)
	}
}

func TestRefusedEventsNeverReachTheStore(t *testing.T) {
	store := &fakeStore{}
	s := start(t, store, testConfig())
	good := signedBody("e1")
	cases := map[string]struct {
		provider, secret string
		body             []byte
		want             error
	}{
		"wrong secret":       {"sms-a", "0000000000000000", good, ports.ErrInvalidSignature},
		"unknown provider":   {"nope", secretA, good, ports.ErrUnknownProvider},
		"provider no key":    {"no-keys", secretA, good, ports.ErrUnknownProvider},
		"malformed":          {"sms-a", secretA, []byte(`{"channel":"sms"}`), ports.ErrInvalidEvent},
		"channel not served": {"sms-a", secretA, []byte(`{"message_id":"m","channel":"email","status":"delivered"}`), ports.ErrInvalidEvent},
		"whatsapp on sms":    {"sms-a", secretA, []byte(`{"message_id":"m","channel":"whatsapp","status":"delivered"}`), ports.ErrInvalidEvent},
	}
	for name, c := range cases {
		if err := receive(s, c.provider, c.secret, c.body); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
	// A bad signature on an otherwise good body is refused before it is parsed.
	if err := s.Receive(context.Background(), "sms-a", good, ""); !errors.Is(err, ports.ErrInvalidSignature) {
		t.Errorf("missing signature: %v", err)
	}
	if calls, _ := store.saved(); calls != 0 {
		t.Errorf("store called %d times", calls)
	}
}

func TestAProviderServesEveryChannelItListed(t *testing.T) {
	store := &fakeStore{}
	s := start(t, store, testConfig())
	for _, ch := range []string{"sms", "whatsapp"} {
		body := []byte(fmt.Sprintf(`{"message_id":"m-%s","channel":"%s","status":"failed"}`, ch, ch))
		if err := receive(s, "multi", "fedcba9876543210", body); err != nil {
			t.Errorf("%s: %v", ch, err)
		}
	}
}

func TestStoreErrorReachesTheRequest(t *testing.T) {
	store := &fakeStore{saveErr: errors.New("database down")}
	s := start(t, store, testConfig())
	err := receive(s, "sms-a", secretA, signedBody("e1"))
	if err == nil || !strings.Contains(err.Error(), "database down") {
		t.Errorf("err = %v", err)
	}
	if errors.Is(err, ports.ErrInvalidEvent) || errors.Is(err, ports.ErrInvalidSignature) {
		t.Error("a store failure must not look like the sender's fault")
	}
	if calls, _ := store.saved(); calls != 1 {
		t.Errorf("a failed single event was tried %d times, want 1", calls)
	}
}

func TestOneBadEventDoesNotFailItsNeighbors(t *testing.T) {
	store := &fakeStore{reject: func(e domain.WebhookEvent) bool { return e.EventID == "bad" }}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 4, time.Minute
	s := start(t, store, cfg)

	ids := []string{"e1", "bad", "e2", "e3"}
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = receive(s, "sms-a", secretA, signedBody(id))
		}()
	}
	wg.Wait()
	for i, id := range ids {
		if (errs[i] != nil) != (id == "bad") {
			t.Errorf("%s: err = %v", id, errs[i])
		}
	}
	if _, events := store.saved(); len(events) != 3 {
		t.Errorf("saved %d events, want the 3 good ones", len(events))
	}
}

func TestIsolationStopsWhenEveryRetryFails(t *testing.T) {
	store := &fakeStore{saveErr: errors.New("database down")}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 6, time.Minute
	s := start(t, store, cfg)

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receive(s, "sms-a", secretA, signedBody(fmt.Sprint("e", i))); err == nil {
				t.Error("nil error with the database down")
			}
		}()
	}
	wg.Wait()
	// One batch try, then single retries until two fail in a row.
	if calls, _ := store.saved(); calls != 1+isolateAfter {
		t.Errorf("store called %d times, want %d", calls, 1+isolateAfter)
	}
}

func TestACancelledRequestStillGetsItsEventSaved(t *testing.T) {
	store := &fakeStore{gate: make(chan struct{})}
	s := start(t, store, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	body := signedBody("e1")
	result := make(chan error, 1)
	go func() { result <- s.Receive(ctx, "sms-a", body, Sign(secretA, body)) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	close(store.gate)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, events := store.saved(); len(events) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the event of the cancelled request was never saved")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAFullQueueHoldsRequestsBackUntilTheirContextEnds(t *testing.T) {
	store := &fakeStore{gate: make(chan struct{})}
	cfg := testConfig()
	cfg.QueueSize, cfg.BatchSize = 1, 1
	s := start(t, store, cfg)
	defer close(store.gate)

	// The flusher holds one event at the gate and the queue holds one more.
	for i := 0; i < 2; i++ {
		go func() { _ = receive(s, "sms-a", secretA, signedBody(fmt.Sprint("fill", i))) }()
	}
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	body := signedBody("late")
	if err := s.Receive(ctx, "sms-a", body, Sign(secretA, body)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the deadline", err)
	}
}

func TestShutdownSavesWhatIsQueuedThenRefusesNewEvents(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 100, time.Minute // the batch only leaves on shutdown
	s, err := New(store, testProviders, cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run(ctx) }()

	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() { results <- receive(s, "sms-a", secretA, signedBody(fmt.Sprint("q", i))) }()
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Errorf("queued event answered with %v", err)
		}
	}
	if err := <-runDone; err != nil {
		t.Errorf("Run = %v", err)
	}
	if _, events := store.saved(); len(events) != 3 {
		t.Errorf("saved %d events, want 3", len(events))
	}
	if err := receive(s, "sms-a", secretA, signedBody("after")); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("after shutdown: err = %v, want ErrShuttingDown", err)
	}
}

func TestRunTwiceIsAnError(t *testing.T) {
	s := start(t, &fakeStore{}, testConfig())
	// An answered event proves the first Run has started, so it holds the once.
	if err := receive(s, "sms-a", secretA, signedBody("e1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err == nil {
		t.Error("second Run returned nil")
	}
}

func TestManyConcurrentRequestsAreAllSaved(t *testing.T) {
	store := &fakeStore{}
	cfg := testConfig()
	cfg.BatchSize, cfg.Flushers, cfg.QueueSize = 16, 4, 64
	s := start(t, store, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receive(s, "sms-a", secretA, signedBody(fmt.Sprint("c", i))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	_, events := store.saved()
	seen := map[string]bool{}
	for _, e := range events {
		seen[e.EventID] = true
	}
	if len(events) != 500 || len(seen) != 500 {
		t.Errorf("saved %d events (%d distinct), want 500", len(events), len(seen))
	}
}

func TestResolverRunsOnATickAndDrainsFullPasses(t *testing.T) {
	store := &fakeStore{resolves: make(chan int, 4)}
	cfg := testConfig()
	cfg.ResolveBatch = 10
	store.resolves <- 10 // full: another pass follows at once
	store.resolves <- 10
	store.resolves <- 3 // short: stop
	start(t, store, cfg)

	deadline := time.Now().Add(2 * time.Second)
	for len(store.resolves) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d answers left unread", len(store.resolves))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 3 passes drained the 3 answers; later ticks keep asking and get 0.
	if n := store.resolved.Load(); n < 3 {
		t.Errorf("ResolvePending called %d times, want at least 3", n)
	}
}

func TestResolverSurvivesAnError(t *testing.T) {
	store := &fakeStore{resolveE: errors.New("database down")}
	start(t, store, testConfig())
	deadline := time.Now().Add(2 * time.Second)
	for store.resolved.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the resolver stopped after an error (%d calls)", store.resolved.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNewRejectsBadSettingsAndProviders(t *testing.T) {
	good := testConfig()
	with := func(f func(*Config)) Config { c := good; f(&c); return c }
	for name, cfg := range map[string]Config{
		"batch size":       with(func(c *Config) { c.BatchSize = 0 }),
		"flushers":         with(func(c *Config) { c.Flushers = 0 }),
		"queue size":       with(func(c *Config) { c.QueueSize = 0 }),
		"flush interval":   with(func(c *Config) { c.FlushInterval = 0 }),
		"flush timeout":    with(func(c *Config) { c.FlushTimeout = 0 }),
		"resolve interval": with(func(c *Config) { c.ResolveInterval = 0 }),
		"resolve batch":    with(func(c *Config) { c.ResolveBatch = 0 }),
	} {
		if _, err := New(&fakeStore{}, testProviders, cfg, discard()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, providers := range map[string]map[string]Provider{
		"no providers":       nil,
		"only empty secrets": {"a": {Channels: []domain.Channel{domain.ChannelSMS}}},
		"short secret":       {"a": {Secret: "short", Channels: []domain.Channel{domain.ChannelSMS}}},
		"secret no channel":  {"a": {Secret: secretA}},
	} {
		if _, err := New(&fakeStore{}, providers, good, discard()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestASlowDatabaseFailsTheBatchWithoutRetryingEachEvent(t *testing.T) {
	store := &fakeStore{saveErr: fmt.Errorf("save: %w", context.DeadlineExceeded)}
	cfg := testConfig()
	cfg.BatchSize, cfg.FlushInterval = 4, time.Minute
	s := start(t, store, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := receive(s, "sms-a", secretA, signedBody(fmt.Sprint("e", i))); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("err = %v", err)
			}
		}()
	}
	wg.Wait()
	if calls, _ := store.saved(); calls != 1 {
		t.Errorf("store called %d times, want 1: a timeout is not a bad event", calls)
	}
}

func TestThrottleAllowsOneLinePerSecondAndCountsTheRest(t *testing.T) {
	var th throttle
	now := time.Now()
	if ok, skipped := th.allow(now); !ok || skipped != 0 {
		t.Fatalf("first call = %v, %d", ok, skipped)
	}
	for i := 0; i < 3; i++ {
		if ok, _ := th.allow(now.Add(time.Duration(i+1) * 100 * time.Millisecond)); ok {
			t.Fatal("allowed within the second")
		}
	}
	if ok, skipped := th.allow(now.Add(time.Second)); !ok || skipped != 3 {
		t.Errorf("after a second = %v, %d skipped, want true, 3", ok, skipped)
	}
	if ok, skipped := th.allow(now.Add(2 * time.Second)); !ok || skipped != 0 {
		t.Errorf("next window = %v, %d skipped", ok, skipped)
	}
}
