package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/eshaar/internal/adapters/httpapi"
	"github.com/mansonxasthur/eshaar/internal/adapters/mock"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/ports"
	"github.com/mansonxasthur/eshaar/internal/sender"
	"github.com/mansonxasthur/eshaar/internal/webhook"
)

// oneShot is a consumer that hands out the batches it is given, so the sender
// runs its real save-then-send flow without a broker.
type oneShot struct{ batches chan []domain.Request }

func (c *oneShot) Poll(ctx context.Context) (ports.Batch, error) {
	select {
	case reqs := <-c.batches:
		return staticBatch(reqs), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type staticBatch []domain.Request

func (b staticBatch) Requests() []domain.Request { return b }
func (staticBatch) Ack(context.Context) error    { return nil }
func (staticBatch) Release()                     {}

func smsRequest(key string) domain.Request {
	return domain.Request{
		IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started", Phone: "+966500000000",
		Content: map[domain.Channel]json.RawMessage{
			domain.ChannelSMS: json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
		},
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// holdAccept delays MarkAccepted until hold returns, so a test can let a
// webhook arrive before the sender has recorded the accept.
type holdAccept struct {
	ports.NotificationStore
	hold func(id string)
}

func (h holdAccept) MarkAccepted(ctx context.Context, id, provider, providerMessageID string) error {
	h.hold(id)
	return h.NotificationStore.MarkAccepted(ctx, id, provider, providerMessageID)
}

// e2e is the webhook process (service, handler, real Postgres) behind an HTTP
// test server, plus what a sender needs to feed it.
type e2e struct {
	t      *testing.T
	db     *pgx.Conn
	dbURL  string
	store  *postgres.Store
	server *httptest.Server

	mu       sync.Mutex
	postErrs []string
}

func startE2E(t *testing.T) *e2e {
	t.Helper()
	db, url := startPostgres(t)
	ctx := context.Background()
	store, err := postgres.NewStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	cfg, err := config.Load(func(k string) string {
		return map[string]string{
			config.EnvDatabaseURL:            url,
			config.EnvWebhookFlushInterval:   "5ms",
			config.EnvWebhookResolveInterval: "20ms",
		}[k]
	}, config.NeedDatabase|config.NeedWebhook)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	svc, err := webhook.New(store, map[string]webhook.Provider{
		"sms-a": {Secret: secret, Channels: []domain.Channel{domain.ChannelSMS}},
	}, webhookConfig(cfg), log)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- svc.Run(runCtx) }()
	srv := httptest.NewServer(httpapi.NewWebhookHandler(svc, log).Routes())
	t.Cleanup(func() {
		srv.Close()
		cancel()
		<-done
	})
	return &e2e{t: t, db: db, dbURL: url, store: store, server: srv}
}

// notify is what a mock provider calls to report an event: it signs the body
// like the provider would and POSTs it to the webhook endpoint. A failure is
// recorded and checked at the end of the test, since this runs on a timer.
func (e *e2e) notify(ev mock.Event) {
	body := ev.JSON()
	code, out := e.post(ev.Provider, webhook.Sign(secret, body), body)
	if code != http.StatusOK {
		e.mu.Lock()
		e.postErrs = append(e.postErrs, out)
		e.mu.Unlock()
	}
}

func (e *e2e) post(provider, signature string, body []byte) (int, string) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, e.server.URL+"/v1/webhooks/"+provider, bytes.NewReader(body))
	if err != nil {
		return 0, err.Error()
	}
	req.Header.Set(httpapi.SignatureHeader, signature)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String()
}

func (e *e2e) checkNoPostErrors() {
	e.t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.postErrs) > 0 {
		e.t.Errorf("webhook posts failed: %v", e.postErrs)
	}
}

// runSender starts a sender over store with the given chain for sms and feeds
// it one request. The returned function stops it.
func (e *e2e) runSender(store ports.NotificationStore, route sender.Route, key string) (stop func()) {
	e.t.Helper()
	consumer := &oneShot{batches: make(chan []domain.Request, 1)}
	s, err := sender.New(consumer, store, map[domain.Channel][]sender.Route{domain.ChannelSMS: {route}}, sender.Config{
		PoolSize: 4, SendTimeout: 3 * time.Second, StatusTimeout: 200 * time.Millisecond, DrainTimeout: 5 * time.Second,
		MinBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	consumer.batches <- []domain.Request{smsRequest(key)}
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				e.t.Errorf("sender: %v", err)
			}
		case <-time.After(30 * time.Second):
			e.t.Error("sender did not stop")
		}
	}
}

type row struct {
	state    string
	reason   *string
	provider *string
	mid      *string
}

func (e *e2e) row(key string) row {
	e.t.Helper()
	var r row
	err := e.db.QueryRow(context.Background(),
		`SELECT state, failure_reason, provider, provider_message_id FROM notifications WHERE idempotency_key = $1 AND channel = 'sms'`, key).
		Scan(&r.state, &r.reason, &r.provider, &r.mid)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		e.t.Fatal(err)
	}
	return r
}

func (e *e2e) events(key string) (n, linked int) {
	e.t.Helper()
	err := e.db.QueryRow(context.Background(),
		`SELECT count(*), count(notification_id) FROM webhook_events WHERE payload->>'idempotency_key' = $1`, key).Scan(&n, &linked)
	if err != nil {
		e.t.Fatal(err)
	}
	return n, linked
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// TestWebhookEndToEnd runs the sender and the webhook process against one
// Postgres, with the mock provider reporting events to the real endpoint.
func TestWebhookEndToEnd(t *testing.T) {
	e := startE2E(t)
	route := func(p ports.Provider) sender.Route { return sender.Route{Provider: p, Timeout: 200 * time.Millisecond} }

	t.Run("a provider that accepts and later reports a failure leaves the row failed", func(t *testing.T) {
		stop := e.runSender(e.store, route(mock.NewProvider("sms-a", mock.WithLateFailure(20*time.Millisecond, e.notify))), "late-fail")
		defer stop()
		eventually(t, "the row to fail", func() bool { return e.row("late-fail").state == "failed" })
		r := e.row("late-fail")
		if str(r.reason) != "mock late failure" || str(r.provider) != "sms-a" || !strings.HasPrefix(str(r.mid), "sms-a-") {
			t.Errorf("row = %+v reason %s provider %s mid %s", r, str(r.reason), str(r.provider), str(r.mid))
		}
		if n, linked := e.events("late-fail"); n != 1 || linked != 1 {
			t.Errorf("events = %d, linked = %d, want 1 and 1", n, linked)
		}
		e.checkNoPostErrors()
	})

	t.Run("a provider that reports a delivery leaves the row delivered", func(t *testing.T) {
		stop := e.runSender(e.store, route(mock.NewProvider("sms-a", mock.WithLateDelivery(20*time.Millisecond, e.notify))), "late-ok")
		defer stop()
		eventually(t, "the row to be delivered", func() bool { return e.row("late-ok").state == "delivered" })
		if r := e.row("late-ok"); r.reason != nil {
			t.Errorf("a delivered row has the reason %q", *r.reason)
		}
		e.checkNoPostErrors()
	})

	t.Run("an event that beats the accept is stored, then applied once the row is accepted", func(t *testing.T) {
		holdDB, err := pgx.Connect(context.Background(), e.dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holdDB.Close(context.Background()) }()
		var stateAtHold string
		var eventsAtHold, linkedAtHold int
		hold := func(id string) {
			// The event is posted as soon as the provider accepts, before this
			// write: wait for it to be stored, and look at what it did.
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				_ = holdDB.QueryRow(context.Background(),
					`SELECT count(*), count(notification_id) FROM webhook_events WHERE payload->>'idempotency_key' = 'early'`).Scan(&eventsAtHold, &linkedAtHold)
				if eventsAtHold > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			_ = holdDB.QueryRow(context.Background(), `SELECT state FROM notifications WHERE id = $1`, id).Scan(&stateAtHold)
		}
		stop := e.runSender(holdAccept{e.store, hold}, route(mock.NewProvider("sms-a", mock.WithLateDelivery(0, e.notify))), "early")
		defer stop()
		eventually(t, "the row to be delivered", func() bool { return e.row("early").state == "delivered" })
		if stateAtHold != "pending" || eventsAtHold != 1 || linkedAtHold != 0 {
			t.Errorf("when the event arrived: row %s, %d events, %d linked; want pending, 1, 0", stateAtHold, eventsAtHold, linkedAtHold)
		}
		eventually(t, "the event to be linked", func() bool { _, linked := e.events("early"); return linked == 1 })
		e.checkNoPostErrors()
	})

	t.Run("sending the same event again is a 200 and changes nothing", func(t *testing.T) {
		var mu sync.Mutex
		var seen []mock.Event
		capture := func(ev mock.Event) {
			mu.Lock()
			seen = append(seen, ev)
			mu.Unlock()
			e.notify(ev)
		}
		stop := e.runSender(e.store, route(mock.NewProvider("sms-a", mock.WithLateFailure(10*time.Millisecond, capture))), "dup")
		defer stop()
		eventually(t, "the row to fail", func() bool { return e.row("dup").state == "failed" })
		mu.Lock()
		ev := seen[0]
		mu.Unlock()
		body := ev.JSON()
		if code, out := e.post("sms-a", webhook.Sign(secret, body), body); code != http.StatusOK {
			t.Errorf("duplicate: %d %s", code, out)
		}
		// The same event id claiming delivery must not change the final state either.
		ev.Status = domain.StateDelivered
		body = ev.JSON()
		if code, out := e.post("sms-a", webhook.Sign(secret, body), body); code != http.StatusOK {
			t.Errorf("duplicate with another status: %d %s", code, out)
		}
		if n, _ := e.events("dup"); n != 1 {
			t.Errorf("%d events stored, want 1", n)
		}
		if r := e.row("dup"); r.state != "failed" {
			t.Errorf("state = %s, want failed", r.state)
		}
	})

	t.Run("a late event moves an unresolved row to its final state and names its provider", func(t *testing.T) {
		// One provider that times out and cannot be asked: the row is unresolved.
		silent := mock.WithoutStatusCheck(mock.NewProvider("sms-a", mock.WithDelay(time.Minute)))
		stop := e.runSender(e.store, route(silent), "unres")
		defer stop()
		eventually(t, "the row to be unresolved", func() bool { return e.row("unres").state == "unresolved" })
		if r := e.row("unres"); r.provider != nil || r.mid != nil || r.reason == nil {
			t.Fatalf("unresolved row = %+v", r)
		}

		body := mock.Event{
			Provider: "sms-a", EventID: "late-evt-1", MessageID: "late-m1", IdempotencyKey: "unres",
			Channel: domain.ChannelSMS, Status: domain.StateDelivered,
		}.JSON()
		if code, out := e.post("sms-a", webhook.Sign(secret, body), body); code != http.StatusOK {
			t.Fatalf("late event: %d %s", code, out)
		}
		r := e.row("unres")
		if r.state != "delivered" || str(r.provider) != "sms-a" || str(r.mid) != "late-m1" || r.reason != nil {
			t.Errorf("row = %+v provider %s mid %s", r, str(r.provider), str(r.mid))
		}
	})

	t.Run("a forged event changes nothing", func(t *testing.T) {
		stop := e.runSender(e.store, route(mock.NewProvider("sms-a")), "forged")
		defer stop()
		eventually(t, "the row to be accepted", func() bool { return e.row("forged").state == "accepted" })
		r := e.row("forged")
		body := mock.Event{
			Provider: "sms-a", EventID: "forged-1", MessageID: str(r.mid), IdempotencyKey: "forged",
			Channel: domain.ChannelSMS, Status: domain.StateFailed, Reason: "forged",
		}.JSON()
		if code, _ := e.post("sms-a", webhook.Sign("ffffffffffffffff", body), body); code != http.StatusUnauthorized {
			t.Errorf("forged event: %d", code)
		}
		if got := e.row("forged").state; got != "accepted" {
			t.Errorf("state = %s after a forged event", got)
		}
		if n, _ := e.events("forged"); n != 0 {
			t.Errorf("%d events stored from a forged post", n)
		}
	})
}

// TestMockEventsAreValidWebhooks holds the mock's wire format and the endpoint's
// parser together: what the mock reports is what ParseEvent reads.
func TestMockEventsAreValidWebhooks(t *testing.T) {
	for _, status := range []domain.State{domain.StateDelivered, domain.StateFailed} {
		ev := mock.Event{
			Provider: "sms-a", EventID: "sms-a-x-evt-1", MessageID: "sms-a-x-1", IdempotencyKey: "k1",
			Channel: domain.ChannelWhatsApp, Status: status, Reason: "الرقم غير صالح",
		}
		got, err := webhook.ParseEvent("sms-a", ev.JSON())
		if err != nil {
			t.Fatalf("%s: %v", status, err)
		}
		if got.EventID != ev.EventID || got.ProviderMessageID != ev.MessageID || got.IdempotencyKey != ev.IdempotencyKey ||
			got.Channel != ev.Channel || got.Status != status || got.Reason != ev.Reason {
			t.Errorf("%s: parsed %+v from %+v", status, got, ev)
		}
	}
}
