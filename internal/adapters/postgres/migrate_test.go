package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	uniqueViolation = "23505"
	checkViolation  = "23514"
)

// newDB starts a throwaway Postgres, applies the migrations and returns a
// connection to it. The test is skipped under -short or without Docker.
func newDB(t *testing.T) *pgx.Conn {
	conn, _ := newDBWithURL(t)
	return conn
}

func newDBWithURL(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Postgres container test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("eshaar"),
		tcpostgres.WithUsername("eshaar"),
		tcpostgres.WithPassword("eshaar"),
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
	if err := MigrateUp(url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn, url
}

func requireUniqueViolation(t *testing.T, err error, constraint string) {
	t.Helper()
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != uniqueViolation || pgErr.ConstraintName != constraint {
		t.Fatalf("err = %v, want unique violation on %s", err, constraint)
	}
}

const insertNotification = `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, phone)
VALUES ($1, 'user-1', 'otp', $2, '{"body":"مرحبا"}', $3, $4)`

func TestNotificationsUniqueOnIdempotencyKeyAndChannel(t *testing.T) {
	conn := newDB(t)
	ctx := context.Background()
	email, phone := "a@example.com", "+966500000000"

	if _, err := conn.Exec(ctx, insertNotification, "key-1", "email", &email, nil); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := conn.Exec(ctx, insertNotification, "key-1", "email", &email, nil)
	requireUniqueViolation(t, err, "notifications_idempotency_channel_key")

	// The same request on another channel is a different row.
	if _, err := conn.Exec(ctx, insertNotification, "key-1", "sms", nil, &phone); err != nil {
		t.Errorf("same key, other channel: %v", err)
	}
}

func requireCheckViolation(t *testing.T, err error, constraint string) {
	t.Helper()
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != checkViolation || pgErr.ConstraintName != constraint {
		t.Fatalf("err = %v, want check violation %s", err, constraint)
	}
}

func TestNotificationsRejectInvalidRows(t *testing.T) {
	conn := newDB(t)
	ctx := context.Background()
	email, phone := "a@example.com", "+966500000000"

	tests := []struct {
		name       string
		channel    string
		email      *string
		phone      *string
		constraint string
	}{
		{"email row without address", "email", nil, nil, "notifications_contact_check"},
		{"email row with phone", "email", &email, &phone, "notifications_contact_check"},
		{"sms row with email", "sms", &email, nil, "notifications_contact_check"},
		{"unknown channel", "fax", &email, nil, "notifications_channel_check"},
	}
	for i, tt := range tests {
		_, err := conn.Exec(ctx, insertNotification, fmt.Sprintf("bad-%d", i), tt.channel, tt.email, tt.phone)
		t.Run(tt.name, func(t *testing.T) { requireCheckViolation(t, err, tt.constraint) })
	}

	if _, err := conn.Exec(ctx, insertNotification, "ok-1", "email", &email, nil); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, `UPDATE notifications SET state = 'bogus'`)
	requireCheckViolation(t, err, "notifications_state_check")

	// An accepted row needs the provider details webhooks match on.
	_, err = conn.Exec(ctx, `UPDATE notifications SET state = 'accepted'`)
	requireCheckViolation(t, err, "notifications_accepted_check")
}

func TestProviderMessageIDUniquePerProvider(t *testing.T) {
	conn := newDB(t)
	ctx := context.Background()
	email := "a@example.com"
	for _, key := range []string{"k1", "k2"} {
		if _, err := conn.Exec(ctx, insertNotification, key, "email", &email, nil); err != nil {
			t.Fatal(err)
		}
	}
	const accept = `UPDATE notifications SET state = 'accepted', accepted_at = now(), provider = $1, provider_message_id = 'msg-1' WHERE idempotency_key = $2`
	if _, err := conn.Exec(ctx, accept, "provider-a", "k1"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, accept, "provider-a", "k2")
	requireUniqueViolation(t, err, "notifications_provider_message_id_idx")
	if _, err := conn.Exec(ctx, accept, "provider-b", "k2"); err != nil {
		t.Errorf("same message id, other provider: %v", err)
	}
}

func TestMigrationsRoundTrip(t *testing.T) {
	_, url := newDBWithURL(t)
	err := withMigrator(url, func(m *migrate.Migrate) error { return m.Down() })
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	if err := MigrateUp(url); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

func TestWebhookEventsUniqueOnProviderAndEventID(t *testing.T) {
	conn := newDB(t)
	ctx := context.Background()
	const insert = `
INSERT INTO webhook_events (provider, channel, event_id, provider_message_id, payload)
VALUES ($1, 'sms', $2, 'msg-1', '{}')`

	if _, err := conn.Exec(ctx, insert, "provider-a", "evt-1"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := conn.Exec(ctx, insert, "provider-a", "evt-1")
	requireUniqueViolation(t, err, "webhook_events_provider_event_key")

	// Event ids are scoped by provider.
	if _, err := conn.Exec(ctx, insert, "provider-b", "evt-1"); err != nil {
		t.Errorf("same event id, other provider: %v", err)
	}
	// NULL event ids never collide (the constraint can't be tested with them).
	for range 2 {
		if _, err := conn.Exec(ctx, insert, "provider-a", nil); err != nil {
			t.Errorf("null event id: %v", err)
		}
	}
}
