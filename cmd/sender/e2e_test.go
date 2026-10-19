package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mansonxasthur/eshaar/internal/adapters/kafka"
	"github.com/mansonxasthur/eshaar/internal/adapters/mock"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/ports"
	"github.com/mansonxasthur/eshaar/internal/sender"
)

// env is a throwaway Kafka and Postgres.
type env struct {
	brokers []string
	dbURL   string
	db      *pgx.Conn
}

func startEnv(t *testing.T) env {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	kctr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	testcontainers.CleanupContainer(t, kctr)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := kctr.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("eshaar"), tcpostgres.WithUsername("eshaar"), tcpostgres.WithPassword("eshaar"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	testcontainers.CleanupContainer(t, pctr)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	url, err := pctr.ConnectionString(ctx, "sslmode=disable")
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
	return env{brokers: brokers, dbURL: url, db: db}
}

func (e env) createTopic(t *testing.T, topic string) {
	t.Helper()
	admin, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := kadm.NewClient(admin).CreateTopic(context.Background(), 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

func (e env) config(topic string) config.Config {
	return config.Config{
		DatabaseURL: e.dbURL, KafkaBrokers: e.brokers, KafkaTopic: topic, KafkaGroupID: "sender-" + topic,
		SenderPoolSize: 8, SenderBatchSize: 100, SenderSendTimeout: 3 * time.Second, ProviderRateLimit: 1000,
	}
}

func request(key string) domain.Request {
	return domain.Request{
		IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started",
		Email: "a@example.com", Phone: "+966500000000",
		Content: map[domain.Channel]json.RawMessage{
			domain.ChannelEmail:    json.RawMessage(`{"template_id":"match_started_v2","variables":{"team_a":"الهلال","team_b":"النصر"}}`),
			domain.ChannelSMS:      json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
			domain.ChannelWhatsApp: json.RawMessage(`{"template_id":"match_started","variables":{"team_a":"الهلال"}}`),
		},
	}
}

func (e env) publish(t *testing.T, topic string, reqs ...domain.Request) {
	t.Helper()
	pub, err := kafka.NewPublisher(e.brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, r := range reqs {
		if err := pub.Publish(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
}

// count returns the number of notification rows matching a WHERE clause.
func (e env) count(t *testing.T, where string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE `+where, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// start runs the sender and returns a function that stops it.
func start(t *testing.T, run func(ctx context.Context) error) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("sender: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("sender did not stop")
		}
	}
}

// TestMessageToRowsAndSends takes a three-channel request from the topic to
// three accepted rows and three provider sends, and checks that a duplicate
// delivery of the same request adds neither rows nor sends.
func TestMessageToRowsAndSends(t *testing.T) {
	e := startEnv(t)
	const topic = "notifications"
	e.createTopic(t, topic)

	provider := mock.NewProvider("mock")
	providers := map[domain.Channel]ports.Provider{}
	for _, c := range domain.Channels() {
		providers[c] = provider
	}
	stop := start(t, func(ctx context.Context) error {
		return serve(ctx, e.config(topic), slog.New(slog.DiscardHandler), providers)
	})
	defer stop()

	e.publish(t, topic, request("match-1"))
	eventually(t, "three accepted rows", func() bool { return e.count(t, `idempotency_key = 'match-1' AND state = 'accepted'`) == 3 })

	rows, err := e.db.Query(context.Background(),
		`SELECT channel, content->>'text', email, phone, provider, provider_message_id FROM notifications WHERE idempotency_key = 'match-1'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var channel, provider, msgID string
		var text, email, phone *string
		if err := rows.Scan(&channel, &text, &email, &phone, &provider, &msgID); err != nil {
			t.Fatal(err)
		}
		seen[channel] = true
		if provider != "mock" || msgID == "" {
			t.Errorf("%s: provider %q message id %q", channel, provider, msgID)
		}
		switch channel {
		case "email":
			if email == nil || phone != nil {
				t.Errorf("email row contacts: %v %v", email, phone)
			}
		case "sms":
			if phone == nil || email != nil || text == nil || *text != "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" {
				t.Errorf("sms row: %v %v %v", phone, email, text)
			}
		case "whatsapp":
			if phone == nil || email != nil {
				t.Errorf("whatsapp row contacts: %v %v", email, phone)
			}
		}
	}
	if len(seen) != 3 {
		t.Errorf("channels saved = %v, want all three", seen)
	}
	if got := len(provider.Sent()); got != 3 {
		t.Fatalf("provider sends = %d, want 3", got)
	}

	// The same key published again, then a new one: the duplicate adds nothing.
	e.publish(t, topic, request("match-1"), request("match-2"))
	eventually(t, "the second request", func() bool { return e.count(t, `idempotency_key = 'match-2' AND state = 'accepted'`) == 3 })
	time.Sleep(2 * time.Second) // let the duplicate be consumed too
	if got := e.count(t, `idempotency_key = 'match-1'`); got != 3 {
		t.Errorf("match-1 rows = %d, want 3", got)
	}
	if got := len(provider.Sent()); got != 6 {
		t.Errorf("provider sends = %d, want 6 (the duplicate must not be sent)", got)
	}
}

// failingStore fails its first InsertPending, as a database outage would.
type failingStore struct {
	ports.NotificationStore
	calls atomic.Int32
}

func (s *failingStore) InsertPending(ctx context.Context, rows []domain.Notification) ([]domain.Notification, error) {
	if s.calls.Add(1) == 1 {
		return nil, errors.New("database unavailable")
	}
	return s.NotificationStore.InsertPending(ctx, rows)
}

// TestFailedInsertIsRedeliveredNotLost checks the save-before-ack rule against
// a real broker: when the insert fails nothing is acknowledged, Kafka hands
// the message out again, and it is then saved and sent exactly once.
func TestFailedInsertIsRedeliveredNotLost(t *testing.T) {
	e := startEnv(t)
	const topic = "redelivery"
	e.createTopic(t, topic)

	pg, err := postgres.NewStore(context.Background(), e.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	store := &failingStore{NotificationStore: pg}
	log := slog.New(slog.DiscardHandler)
	cfg := e.config(topic)
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, topic, cfg.KafkaGroupID, cfg.SenderBatchSize, log)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	provider := mock.NewProvider("mock")
	providers := map[domain.Channel]ports.Provider{}
	for _, c := range domain.Channels() {
		providers[c] = provider
	}
	s, err := sender.New(consumer, store, providers, sender.Config{
		PoolSize: 8, SendTimeout: 3 * time.Second, DrainTimeout: 5 * time.Second,
		MinBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s.Run)
	defer stop()

	e.publish(t, topic, request("retry-1"))
	eventually(t, "three accepted rows after the retry", func() bool { return e.count(t, `idempotency_key = 'retry-1' AND state = 'accepted'`) == 3 })
	if store.calls.Load() < 2 {
		t.Errorf("InsertPending calls = %d, want the failed one and a retry", store.calls.Load())
	}
	if got := len(provider.Sent()); got != 3 {
		t.Errorf("provider sends = %d, want 3", got)
	}
}
