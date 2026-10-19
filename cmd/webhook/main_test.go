package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mansonxasthur/eshaar/internal/adapters/httpapi"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/webhook"
)

const secret = "0123456789abcdef"

func providerSpec(name, secret string) config.Provider {
	return config.Provider{Name: name, Priority: 1, RPS: 1, Timeout: time.Second, WebhookSecret: config.Secret(secret)}
}

func TestWebhookProvidersCollectsSecretsAndChannels(t *testing.T) {
	got, err := webhookProviders(config.Providers{
		"sms":      {providerSpec("sms-a", secret), providerSpec("sms-b", "")},
		"email":    {providerSpec("email-a", secret+"1")},
		"whatsapp": {providerSpec("multi", secret)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := got["sms-a"]; p.Secret != secret || len(p.Channels) != 1 || p.Channels[0] != domain.ChannelSMS {
		t.Errorf("sms-a = %+v", p)
	}
	if p := got["sms-b"]; p.Secret != "" {
		t.Errorf("sms-b = %+v", p)
	}
	if p := got["email-a"]; p.Secret != secret+"1" || p.Channels[0] != domain.ChannelEmail {
		t.Errorf("email-a = %+v", p)
	}
}

func TestWebhookProvidersMergesAProviderListedUnderTwoChannels(t *testing.T) {
	got, err := webhookProviders(config.Providers{
		"sms":      {providerSpec("multi", secret)},
		"whatsapp": {providerSpec("multi", secret)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := got["multi"]; len(p.Channels) != 2 || p.Channels[0] != domain.ChannelSMS || p.Channels[1] != domain.ChannelWhatsApp {
		t.Errorf("multi = %+v", p)
	}
}

func TestWebhookProvidersRejectsDifferentSecretsForOneName(t *testing.T) {
	_, err := webhookProviders(config.Providers{
		"sms":      {providerSpec("multi", secret)},
		"whatsapp": {providerSpec("multi", secret+"x")},
	})
	if err == nil || !strings.Contains(err.Error(), "multi") || strings.Contains(err.Error(), secret) {
		t.Errorf("err = %v", err)
	}
}

func TestWebhookProvidersRejectsAnUnknownChannel(t *testing.T) {
	if _, err := webhookProviders(config.Providers{"fax": {providerSpec("fax-a", secret)}}); err == nil {
		t.Error("unknown channel accepted")
	}
}

func TestCheckProvidersNeedsAtLeastOneSecret(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	if err := checkProviders(log, map[string]webhook.Provider{"a": {Channels: []domain.Channel{domain.ChannelSMS}}}); err == nil {
		t.Error("no secrets accepted")
	}
	if err := checkProviders(log, nil); err == nil {
		t.Error("no providers accepted")
	}
	if err := checkProviders(log, map[string]webhook.Provider{
		"a": {Secret: secret, Channels: []domain.Channel{domain.ChannelSMS}},
		"b": {Channels: []domain.Channel{domain.ChannelSMS}},
	}); err != nil {
		t.Errorf("one secret is enough: %v", err)
	}
}

func startPostgres(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("eshaar"), tcpostgres.WithUsername("eshaar"), tcpostgres.WithPassword("eshaar"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.MigrateUp(url); err != nil {
		t.Fatal(err)
	}
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(ctx) })
	return db, url
}

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func post(t *testing.T, url, signature, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if signature != "" {
		req.Header.Set(httpapi.SignatureHeader, signature)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// healthz reports whether the server answers GET /healthz.
func healthz(t *testing.T, base string) bool {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/healthz", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

// TestServeStoresASignedEventAndStopsCleanly runs the wired process against a
// real Postgres: a signed event reaches the row, a forged one does not, and
// cancelling the context stops the server and the service in order.
func TestServeStoresASignedEventAndStopsCleanly(t *testing.T) {
	db, url := startPostgres(t)
	ctx := context.Background()
	var rowID string
	err := db.QueryRow(ctx, `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, phone, state, provider, provider_message_id, accepted_at)
VALUES ('k1', 'u1', 'match_started', 'sms', '{}', '+966500000000', 'accepted', 'sms-a', 'm1', now())
RETURNING id`).Scan(&rowID)
	if err != nil {
		t.Fatal(err)
	}

	store, err := postgres.NewStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg, err := config.Load(func(k string) string {
		return map[string]string{config.EnvDatabaseURL: url}[k]
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

	addr := freeAddr(t)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- serve(runCtx, addr, svc, log) }()
	base := "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for !healthz(t, base) {
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}

	body := `{"event_id":"e1","message_id":"m1","idempotency_key":"k1","channel":"sms","status":"failed","reason":"الرقم غير صالح"}`
	if code, out := post(t, base+"/v1/webhooks/sms-a", webhook.Sign("ffffffffffffffff", []byte(body)), body); code != http.StatusUnauthorized {
		t.Errorf("forged event: %d %s", code, out)
	}
	if code, out := post(t, base+"/v1/webhooks/nope", webhook.Sign(secret, []byte(body)), body); code != http.StatusUnauthorized {
		t.Errorf("unknown provider: %d %s", code, out)
	}
	var state string
	if err := db.QueryRow(ctx, `SELECT state FROM notifications WHERE id = $1`, rowID).Scan(&state); err != nil || state != "accepted" {
		t.Fatalf("a refused event changed the row: %s, %v", state, err)
	}

	if code, out := post(t, base+"/v1/webhooks/sms-a", webhook.Sign(secret, []byte(body)), body); code != http.StatusOK {
		t.Fatalf("signed event: %d %s", code, out)
	}
	var reason *string
	if err := db.QueryRow(ctx, `SELECT state, failure_reason FROM notifications WHERE id = $1`, rowID).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || reason == nil || *reason != "الرقم غير صالح" {
		t.Errorf("row = %s %v, want failed with the Arabic reason", state, reason)
	}
	// The same event again is a 200 and changes nothing.
	if code, _ := post(t, base+"/v1/webhooks/sms-a", webhook.Sign(secret, []byte(body)), body); code != http.StatusOK {
		t.Errorf("duplicate event: %d", code)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM webhook_events`).Scan(&n); err != nil || n != 1 {
		t.Errorf("%d events stored, want 1 (%v)", n, err)
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not stop")
	}
	if healthz(t, base) {
		t.Error("server still answering after shutdown")
	}
}
