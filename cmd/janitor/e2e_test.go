package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mansonxasthur/eshaar/internal/adapters/mock"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/janitor"
	"github.com/mansonxasthur/eshaar/internal/sender"
)

// startDB is a throwaway Postgres with the migrations applied. The janitor
// needs nothing else: it reads the table the sender already filled.
func startDB(t *testing.T) (*postgres.Store, *pgx.Conn) {
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
	store, err := postgres.NewStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(ctx) })
	return store, db
}

func request(key string, channels ...domain.Channel) domain.Request {
	content := map[domain.Channel]json.RawMessage{
		domain.ChannelEmail:    json.RawMessage(`{"template_id":"match_started_v2","variables":{"team_a":"الهلال"}}`),
		domain.ChannelSMS:      json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
		domain.ChannelWhatsApp: json.RawMessage(`{"template_id":"match_started"}`),
	}
	req := domain.Request{IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started",
		Email: "a@example.com", Phone: "+966500000000", Content: map[domain.Channel]json.RawMessage{}}
	for _, c := range channels {
		req.Content[c] = content[c]
	}
	return req
}

// strand saves requests the way the sender does, as pending rows created a
// minute ago, and returns the rows. This is the state a sender crash between
// the ack and the send leaves behind.
func strand(t *testing.T, store *postgres.Store, reqs ...domain.Request) []domain.Notification {
	t.Helper()
	var rows []domain.Notification
	for _, r := range reqs {
		rows = append(rows, r.Notifications(time.Now().Add(-time.Minute))...)
	}
	saved, err := store.InsertPending(context.Background(), rows)
	if err != nil || len(saved) != len(rows) {
		t.Fatalf("insert: %d of %d rows, %v", len(saved), len(rows), err)
	}
	return saved
}

type panicking struct {
	name  string
	calls atomic.Int32
}

func (p *panicking) Name() string { return p.name }
func (p *panicking) Send(context.Context, domain.Notification) (string, error) {
	p.calls.Add(1)
	panic("adapter bug")
}

// startJanitor runs a janitor with short windows over the given chains and
// stops it when the test ends.
func startJanitor(t *testing.T, store *postgres.Store, chains map[domain.Channel][]sender.Route) {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	d, err := sender.NewDeliverer(store, chains, sender.DelivererConfig{SendTimeout: 200 * time.Millisecond, StatusTimeout: 50 * time.Millisecond}, log)
	if err != nil {
		t.Fatal(err)
	}
	j, err := janitor.New(store, d, janitor.Config{
		Interval: 50 * time.Millisecond, SweepInterval: 100 * time.Millisecond, StaleAfter: 500 * time.Millisecond,
		BatchSize: 10, Concurrency: 10, RedriveBudget: 300 * time.Millisecond,
	}, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = j.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type rowState struct {
	state    string
	provider *string
	msgID    *string
	reason   *string
	attempts int
}

func rowOf(t *testing.T, db *pgx.Conn, id string) rowState {
	t.Helper()
	var r rowState
	err := db.QueryRow(context.Background(),
		`SELECT state, provider, provider_message_id, failure_reason, attempts FROM notifications WHERE id = $1`, id).
		Scan(&r.state, &r.provider, &r.msgID, &r.reason, &r.attempts)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func allIn(t *testing.T, db *pgx.Conn, rows []domain.Notification, state string) bool {
	t.Helper()
	for _, n := range rows {
		if rowOf(t, db, n.ID).state != state {
			return false
		}
	}
	return true
}

func mockChains(providers map[domain.Channel]*mock.Provider) map[domain.Channel][]sender.Route {
	chains := map[domain.Channel][]sender.Route{}
	for c, p := range providers {
		chains[c] = []sender.Route{{Provider: p, Timeout: 200 * time.Millisecond}}
	}
	return chains
}

// A sender that crashed after the ack leaves pending rows nobody sent. The
// janitor finds them once they are stale and delivers every channel once.
func TestJanitorRecoversRowsAfterASenderCrash(t *testing.T) {
	store, db := startDB(t)
	rows := strand(t, store, request("k1", domain.ChannelEmail, domain.ChannelSMS, domain.ChannelWhatsApp), request("k2", domain.ChannelSMS))
	providers := map[domain.Channel]*mock.Provider{
		domain.ChannelEmail: mock.NewProvider("email-a"), domain.ChannelSMS: mock.NewProvider("sms-a"), domain.ChannelWhatsApp: mock.NewProvider("wa-a"),
	}
	startJanitor(t, store, mockChains(providers))

	eventually(t, "every row accepted", func() bool { return allIn(t, db, rows, "accepted") })

	for _, n := range rows {
		if r := rowOf(t, db, n.ID); r.attempts != 1 || r.provider == nil || r.msgID == nil {
			t.Errorf("%s/%s = %+v, want accepted on the first attempt with a provider and message id", n.IdempotencyKey, n.Channel, r)
		}
	}
	if got := len(providers[domain.ChannelSMS].Sent()); got != 2 {
		t.Errorf("sms provider took %d messages, want 2", got)
	}
}

// A sender that crashed after a provider took the message, before the outcome
// was written, left last_provider behind. The janitor asks that provider
// first and accepts with its message id, sending nothing a second time.
func TestJanitorAcceptsByStatusCheckWithoutSendingAgain(t *testing.T) {
	store, db := startDB(t)
	rows := strand(t, store, request("k1", domain.ChannelSMS))
	smsA := mock.NewProvider("sms-a")
	took, err := smsA.Send(context.Background(), rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordLastProvider(context.Background(), rows[0].ID, "sms-a"); err != nil {
		t.Fatal(err)
	}
	startJanitor(t, store, mockChains(map[domain.Channel]*mock.Provider{domain.ChannelSMS: smsA}))

	eventually(t, "row accepted", func() bool { return allIn(t, db, rows, "accepted") })

	if r := rowOf(t, db, rows[0].ID); r.msgID == nil || *r.msgID != took {
		t.Errorf("message id = %v, want the one the provider already issued (%s)", r.msgID, took)
	}
	if got := len(smsA.Sent()); got != 1 {
		t.Errorf("provider took %d messages, want only the original", got)
	}
}

// A row whose re-drive never completes (here the adapter always panics) is
// claimed 3 times, then the sweeper gives up on it. The panics must not stop
// the janitor from recovering other rows.
func TestJanitorSweepsRowsThatNeverComplete(t *testing.T) {
	store, db := startDB(t)
	stuck := strand(t, store, request("stuck", domain.ChannelEmail))
	fine := strand(t, store, request("fine", domain.ChannelSMS))
	bad := &panicking{name: "email-a"}
	chains := mockChains(map[domain.Channel]*mock.Provider{domain.ChannelSMS: mock.NewProvider("sms-a")})
	chains[domain.ChannelEmail] = []sender.Route{{Provider: bad, Timeout: 200 * time.Millisecond}}
	startJanitor(t, store, chains)

	eventually(t, "stuck row unresolved", func() bool { return allIn(t, db, stuck, "unresolved") })

	r := rowOf(t, db, stuck[0].ID)
	if r.attempts != 3 || r.reason == nil || *r.reason == "" {
		t.Errorf("stuck row = %+v, want 3 attempts and a reason", r)
	}
	if got := bad.calls.Load(); got != 3 {
		t.Errorf("adapter called %d times, want one per attempt (3)", got)
	}
	if !allIn(t, db, fine, "accepted") {
		t.Error("a panicking adapter stopped the other rows from being recovered")
	}
}

// Two janitors over the same stranded rows must send each message exactly once.
func TestTwoJanitorsSendEachRowOnce(t *testing.T) {
	store, db := startDB(t)
	var reqs []domain.Request
	for i := range 60 {
		reqs = append(reqs, request(fmt.Sprintf("k%d", i), domain.ChannelSMS))
	}
	rows := strand(t, store, reqs...)
	one, two := mock.NewProvider("sms-a"), mock.NewProvider("sms-a")
	startJanitor(t, store, mockChains(map[domain.Channel]*mock.Provider{domain.ChannelSMS: one}))
	startJanitor(t, store, mockChains(map[domain.Channel]*mock.Provider{domain.ChannelSMS: two}))

	eventually(t, "every row accepted", func() bool { return allIn(t, db, rows, "accepted") })

	seen := map[string]int{}
	for _, p := range []*mock.Provider{one, two} {
		for _, n := range p.Sent() {
			seen[n.IdempotencyKey]++
		}
	}
	if len(seen) != len(rows) {
		t.Errorf("%d distinct messages sent, want %d", len(seen), len(rows))
	}
	for key, c := range seen {
		if c != 1 {
			t.Errorf("%s sent %d times", key, c)
		}
	}
}
