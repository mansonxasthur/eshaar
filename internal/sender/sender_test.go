package sender

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// recorder keeps the order of calls made by the fakes.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) count(prefix string) int {
	n := 0
	for _, e := range r.list() {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

type fakeBatch struct {
	reqs   []domain.Request
	rec    *recorder
	ackErr error
}

func (b *fakeBatch) Requests() []domain.Request { return b.reqs }
func (b *fakeBatch) Release()                   { b.rec.add("release") }
func (b *fakeBatch) Ack(context.Context) error {
	b.rec.add("ack")
	return b.ackErr
}

// fakeConsumer hands out its batches in order, then blocks until ctx is done.
type fakeConsumer struct {
	rec     *recorder
	mu      sync.Mutex
	batches []*fakeBatch
}

func (c *fakeConsumer) Poll(ctx context.Context) (ports.Batch, error) {
	c.rec.add("poll")
	c.mu.Lock()
	if len(c.batches) > 0 {
		b := c.batches[0]
		c.batches = c.batches[1:]
		c.mu.Unlock()
		return b, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

// fakeStore models the unique constraint on (idempotency key, channel).
type fakeStore struct {
	rec        *recorder
	mu         sync.Mutex
	rows       map[string]domain.Notification // by key|channel
	insertErrs []error                        // returned by successive InsertPending calls
	accepted   map[string][2]string           // id -> provider, message id
	failed     map[string]string              // id -> reason
	unresolved map[string]string              // id -> reason
}

func newStore(rec *recorder) *fakeStore {
	return &fakeStore{rec: rec, rows: map[string]domain.Notification{}, accepted: map[string][2]string{}, failed: map[string]string{}, unresolved: map[string]string{}}
}

func (s *fakeStore) InsertPending(_ context.Context, rows []domain.Notification) ([]domain.Notification, error) {
	s.rec.add("insert")
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.insertErrs) > 0 {
		err := s.insertErrs[0]
		s.insertErrs = s.insertErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	var out []domain.Notification
	for _, n := range rows {
		k := n.IdempotencyKey + "|" + string(n.Channel)
		if _, dup := s.rows[k]; dup {
			continue
		}
		n.ID = k
		s.rows[k] = n
		out = append(out, n)
	}
	return out, nil
}

func (s *fakeStore) MarkAccepted(_ context.Context, id, provider, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepted[id] = [2]string{provider, msgID}
	return nil
}

func (s *fakeStore) MarkFailed(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[id] = reason
	return nil
}

func (s *fakeStore) MarkUnresolved(_ context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unresolved[id] = reason
	return nil
}

type fakeProvider struct {
	rec  *recorder
	send func(ctx context.Context, n domain.Notification) (string, error)
}

func (p *fakeProvider) Name() string { return "fake" }
func (p *fakeProvider) Send(ctx context.Context, n domain.Notification) (string, error) {
	p.rec.add("send:" + n.IdempotencyKey + ":" + string(n.Channel))
	if p.send != nil {
		return p.send(ctx, n)
	}
	return "msg-" + string(n.Channel), nil
}

func req(key string, channels ...domain.Channel) domain.Request {
	r := domain.Request{
		IdempotencyKey: key, RecipientID: "u1", NotificationType: "t",
		Email: "a@example.com", Phone: "+966500000000",
		Content: map[domain.Channel]json.RawMessage{},
	}
	for _, c := range channels {
		r.Content[c] = json.RawMessage(`{"text":"مرحبا"}`)
	}
	return r
}

func testConfig() Config {
	return Config{PoolSize: 4, SendTimeout: time.Second, StatusTimeout: time.Second, DrainTimeout: time.Second, MinBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
}

type harness struct {
	rec      *recorder
	consumer *fakeConsumer
	store    *fakeStore
	provider *fakeProvider
	sender   *Sender
}

func newHarness(t *testing.T, cfg Config, batches ...*fakeBatch) *harness {
	t.Helper()
	rec := &recorder{}
	for _, b := range batches {
		b.rec = rec
	}
	h := &harness{rec: rec, consumer: &fakeConsumer{rec: rec, batches: batches}, store: newStore(rec), provider: &fakeProvider{rec: rec}}
	providers := map[domain.Channel][]Route{}
	for _, c := range domain.Channels() {
		providers[c] = []Route{{Provider: h.provider, Timeout: time.Second}}
	}
	s, err := New(h.consumer, h.store, providers, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	h.sender = s
	return h
}

// run starts the sender and returns a stop function that cancels it and waits.
func (h *harness) run(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.sender.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return")
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSavesBeforeAckThenSends(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelEmail, domain.ChannelSMS)}})
	stop := h.run(t)
	eventually(t, "two accepted rows", func() bool { h.store.mu.Lock(); defer h.store.mu.Unlock(); return len(h.store.accepted) == 2 })
	stop()

	ev := h.rec.list()
	if ev[0] != "poll" || ev[1] != "insert" || ev[2] != "ack" {
		t.Fatalf("want poll, insert, ack first, got %v", ev)
	}
	if got := h.store.accepted["k1|email"]; got != [2]string{"fake", "msg-email"} {
		t.Errorf("email outcome = %v", got)
	}
	if h.rec.count("send:") != 2 || h.rec.count("release") != 0 {
		t.Errorf("events = %v", ev)
	}
}

func TestInsertFailureAcksNothingAndRedelivers(t *testing.T) {
	batch := &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}}
	h := newHarness(t, testConfig(), batch)
	h.store.insertErrs = []error{errors.New("db down")}
	// A real consumer redelivers a released batch; the fake does the same.
	h.consumer.batches = append(h.consumer.batches, &fakeBatch{reqs: batch.reqs, rec: h.rec})
	stop := h.run(t)
	eventually(t, "the redelivered batch to be sent", func() bool { return h.rec.count("send:") == 1 })
	stop()

	ev := h.rec.list()
	want := []string{"poll", "insert", "release", "poll", "insert", "ack"}
	for i, w := range want {
		if ev[i] != w {
			t.Fatalf("events = %v, want prefix %v", ev, want)
		}
	}
	if h.rec.count("ack") != 1 {
		t.Errorf("acks = %d, want 1 (only after the successful insert)", h.rec.count("ack"))
	}
}

func TestRedeliveredRowsAreNotSentTwice(t *testing.T) {
	same := []domain.Request{req("k1", domain.ChannelEmail, domain.ChannelSMS)}
	h := newHarness(t, testConfig(), &fakeBatch{reqs: same}, &fakeBatch{reqs: same})
	stop := h.run(t)
	eventually(t, "both batches handled", func() bool { return h.rec.count("ack") == 2 })
	stop()
	if got := h.rec.count("send:"); got != 2 {
		t.Errorf("sends = %d, want 2 (conflicting rows are already saved, not sent again)", got)
	}
}

func TestAckFailureStillSendsSavedRows(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}, ackErr: errors.New("commit failed")})
	stop := h.run(t)
	eventually(t, "the send", func() bool { return h.rec.count("send:") == 1 })
	stop()
}

func TestOnlyNewRowsOfAMixedBatchAreSent(t *testing.T) {
	h := newHarness(t, testConfig(),
		&fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}},
		&fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS), req("k2", domain.ChannelSMS)}})
	stop := h.run(t)
	eventually(t, "both batches handled", func() bool { return h.rec.count("ack") == 2 })
	stop()
	if h.rec.count("send:k1") != 1 || h.rec.count("send:k2") != 1 {
		t.Errorf("events = %v", h.rec.list())
	}
}

func TestProviderErrorMarksFailed(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}})
	h.provider.send = func(context.Context, domain.Notification) (string, error) { return "", errors.New("boom") }
	stop := h.run(t)
	eventually(t, "a failed row", func() bool { h.store.mu.Lock(); defer h.store.mu.Unlock(); return len(h.store.failed) == 1 })
	stop()
	if got := h.store.failed["k1|sms"]; got != "fake: boom" {
		t.Errorf("failure reason = %q", got)
	}
}

func TestSendTimeoutMarksUnresolved(t *testing.T) {
	cfg := testConfig()
	cfg.SendTimeout = 20 * time.Millisecond
	h := newHarness(t, cfg, &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}})
	h.provider.send = func(ctx context.Context, _ domain.Notification) (string, error) { <-ctx.Done(); return "", ctx.Err() }
	stop := h.run(t)
	eventually(t, "an unresolved row", func() bool { h.store.mu.Lock(); defer h.store.mu.Unlock(); return len(h.store.unresolved) == 1 })
	stop()
	if got := h.store.unresolved["k1|sms"]; !strings.Contains(got, "deadline exceeded") {
		t.Errorf("failure reason = %q", got)
	}
}

func TestMissingProviderMarksFailed(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}})
	delete(h.sender.providers, domain.ChannelSMS)
	stop := h.run(t)
	eventually(t, "a failed row", func() bool { h.store.mu.Lock(); defer h.store.mu.Unlock(); return len(h.store.failed) == 1 })
	stop()
}

// A full pool must stop the loop from polling, so the backlog stays in Kafka.
func TestFullPoolStopsPolling(t *testing.T) {
	cfg := testConfig()
	cfg.PoolSize = 1
	h := newHarness(t, cfg,
		&fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS, domain.ChannelEmail)}},
		&fakeBatch{reqs: []domain.Request{req("k2", domain.ChannelSMS)}})
	release := make(chan struct{})
	h.provider.send = func(ctx context.Context, _ domain.Notification) (string, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "m", nil
	}
	stop := h.run(t)
	// The first send holds the only slot, so the second row of batch one waits
	// in dispatch and the loop never reaches a second poll.
	eventually(t, "the first send", func() bool { return h.rec.count("send:") == 1 })
	time.Sleep(50 * time.Millisecond)
	if got := h.rec.count("poll"); got != 1 {
		t.Fatalf("polls while the pool was full = %d, want 1", got)
	}
	close(release)
	eventually(t, "all three sends", func() bool { return h.rec.count("send:") == 3 })
	stop()
	if h.rec.count("poll") < 2 {
		t.Errorf("polling did not resume once slots freed: %v", h.rec.list())
	}
}

func TestShutdownWaitsForInFlightSends(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}})
	started := make(chan struct{})
	h.provider.send = func(context.Context, domain.Notification) (string, error) {
		close(started)
		time.Sleep(50 * time.Millisecond)
		return "late", nil
	}
	stop := h.run(t)
	<-started
	stop() // cancels Run's context mid-send and waits for it to return
	if got := h.store.accepted["k1|sms"]; got[1] != "late" {
		t.Errorf("in-flight send was not recorded before shutdown: %v", h.store.accepted)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	var bad []Config
	with := func(change func(*Config)) {
		cfg := testConfig()
		change(&cfg)
		bad = append(bad, cfg)
	}
	with(func(c *Config) { c.PoolSize = 0 })
	with(func(c *Config) { c.SendTimeout = 0 })
	with(func(c *Config) { c.StatusTimeout = 0 })
	with(func(c *Config) { c.MinBackoff = 0 })
	with(func(c *Config) { c.MinBackoff, c.MaxBackoff = 2, 1 })
	for i, cfg := range bad {
		if _, err := New(nil, nil, nil, cfg, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
	badRoutes := map[string][]Route{
		"nil provider": {{Timeout: time.Second}},
		"no timeout":   {{Provider: &fakeProvider{}}},
	}
	for name, chain := range badRoutes {
		providers := map[domain.Channel][]Route{domain.ChannelSMS: chain}
		if _, err := New(nil, nil, providers, testConfig(), slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("%s: route accepted", name)
		}
	}
}

func TestRateLimitThrottlesProvider(t *testing.T) {
	p := WithRateLimit(&fakeProvider{rec: &recorder{}}, 20) // burst 20, then one per 50ms
	n := domain.Notification{IdempotencyKey: "k", Channel: domain.ChannelSMS}
	start := time.Now()
	for range 22 {
		if _, err := p.Send(context.Background(), n); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Errorf("22 calls at 20/s took %v, want the last two throttled", elapsed)
	}
}

func TestRateLimitWaitHonoursTimeout(t *testing.T) {
	p := WithRateLimit(&fakeProvider{rec: &recorder{}}, 1)
	n := domain.Notification{Channel: domain.ChannelSMS}
	if _, err := p.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.Send(ctx, n); err == nil {
		t.Error("second call within the limit window should fail once the context expires")
	}
}

// A provider that panics must not crash the sender; the row stays pending.
func TestProviderPanicLeavesRowPending(t *testing.T) {
	h := newHarness(t, testConfig(), &fakeBatch{reqs: []domain.Request{req("k1", domain.ChannelSMS)}})
	h.provider.send = func(context.Context, domain.Notification) (string, error) { panic("boom") }
	stop := h.run(t)
	eventually(t, "the send to start", func() bool { return h.rec.count("send:k1") == 1 })
	stop()
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if len(h.store.accepted)+len(h.store.failed)+len(h.store.unresolved) != 0 {
		t.Error("a panicking send was recorded")
	}
}
