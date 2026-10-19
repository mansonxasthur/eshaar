package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/adapters/kafka"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
)

const topic = "notifications"

const body = `{
  "idempotency_key": "match-1",
  "recipient_id": "u1",
  "notification_type": "match_started",
  "phone": "+966500000000",
  "content": {"sms": {"text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}}
}`

func startServer(t *testing.T, brokers []string) *httptest.Server {
	t.Helper()
	pub, err := kafka.NewPublisher(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	cfg := config.Config{APIKeys: []string{"dev"}, RateLimitRPS: 100, RateLimitBurst: 100}
	h, err := routes(cfg, pub, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// postJSON returns the response status and body.
func postJSON(t *testing.T, url, key, payload string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url+"/v1/notifications", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// TestRequestToTopic sends a request through the real handler chain and the
// real publisher and reads the message back from a Kafka container.
func TestRequestToTopic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Kafka container test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	srv := startServer(t, brokers)

	// Rejected requests must not reach the topic.
	if got, _ := postJSON(t, srv.URL, "", body); got != http.StatusUnauthorized {
		t.Fatalf("no key: status %d, want 401", got)
	}
	if got, _ := postJSON(t, srv.URL, "dev", `{"idempotency_key":"bad"}`); got != http.StatusBadRequest {
		t.Fatalf("invalid body: status %d, want 400", got)
	}

	if status, resp := postJSON(t, srv.URL, "dev", body); status != http.StatusAccepted {
		t.Fatalf("status %d, want 202: %s", status, resp)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetches := consumer.PollRecords(ctx, 10)
	if err := fetches.Err(); err != nil {
		t.Fatalf("consume: %v", err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("%d records on the topic, want exactly 1 (the 202)", len(recs))
	}
	if string(recs[0].Key) != "match-1" {
		t.Errorf("key = %q, want the idempotency key", recs[0].Key)
	}
	if !strings.Contains(string(recs[0].Value), "مباراة الهلال والنصر تبدأ الساعة 8 مساءً") {
		t.Errorf("Arabic text changed in flight: %s", recs[0].Value)
	}
}

// TestNoBrokerNever202 checks that when Kafka cannot confirm, the caller gets 503.
func TestNoBrokerNever202(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the publish timeout; skipped in -short mode")
	}
	srv := startServer(t, []string{"127.0.0.1:1"})
	if got, _ := postJSON(t, srv.URL, "dev", body); got != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", got)
	}
}
