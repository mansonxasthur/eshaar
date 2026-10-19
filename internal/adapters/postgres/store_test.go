package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

func newStore(t *testing.T) (*Store, *pgx.Conn) {
	t.Helper()
	conn, url := newDBWithURL(t)
	s, err := NewStore(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, conn
}

func rows(key string) []domain.Notification {
	email, phone := "a@example.com", "+966500000000"
	now := time.Now().UTC().Truncate(time.Microsecond)
	return []domain.Notification{
		{IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started", Channel: domain.ChannelEmail,
			Content: json.RawMessage(`{"template_id":"t","variables":{"team_a":"الهلال"}}`), Email: &email, State: domain.StatePending, CreatedAt: now},
		{IdempotencyKey: key, RecipientID: "u1", NotificationType: "match_started", Channel: domain.ChannelSMS,
			Content: json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`), Phone: &phone, State: domain.StatePending, CreatedAt: now},
	}
}

func stateOf(t *testing.T, conn *pgx.Conn, id string) (state string, reason, provider *string) {
	t.Helper()
	err := conn.QueryRow(context.Background(),
		`SELECT state, failure_reason, provider FROM notifications WHERE id = $1`, id).Scan(&state, &reason, &provider)
	if err != nil {
		t.Fatal(err)
	}
	return state, reason, provider
}

func TestInsertPendingSavesOneRowPerChannel(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()

	got, err := s.InsertPending(ctx, rows("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("inserted %d rows, want 2", len(got))
	}
	for _, n := range got {
		if n.ID == "" {
			t.Errorf("%s: no id returned", n.Channel)
		}
	}

	var content, email, phone *string
	var state string
	err = conn.QueryRow(ctx, `SELECT content::text, email, phone, state FROM notifications WHERE id = $1`, got[1].ID).
		Scan(&content, &email, &phone, &state)
	if err != nil {
		t.Fatal(err)
	}
	if state != "pending" || email != nil || phone == nil || *phone != "+966500000000" {
		t.Errorf("sms row = state %s email %v phone %v", state, email, phone)
	}
	if want := rows("k1")[1].Content; !jsonEqual(t, *content, string(want)) {
		t.Errorf("content = %s, want %s", *content, want)
	}
	// Arabic must be stored as UTF-8, not as \u escapes.
	var raw string
	if err := conn.QueryRow(ctx, `SELECT content->>'text' FROM notifications WHERE id = $1`, got[1].ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" {
		t.Errorf("text = %q", raw)
	}
}

func jsonEqual(t *testing.T, a, b string) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal([]byte(a), &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b), &y); err != nil {
		t.Fatal(err)
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xb) == string(yb)
}

func TestInsertPendingTreatsExistingRowsAsSaved(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()

	if _, err := s.InsertPending(ctx, rows("k1")); err != nil {
		t.Fatal(err)
	}
	// A redelivered batch plus one new request: only the new rows come back.
	again, err := s.InsertPending(ctx, append(rows("k1"), rows("k2")...))
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 2 {
		t.Fatalf("inserted %d rows, want only k2's 2", len(again))
	}
	for _, n := range again {
		if n.IdempotencyKey != "k2" {
			t.Errorf("returned an already saved row: %s/%s", n.IdempotencyKey, n.Channel)
		}
	}
	var total int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM notifications`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 4 {
		t.Errorf("table has %d rows, want 4", total)
	}
}

func TestInsertPendingSameKeyTwiceInOneBatch(t *testing.T) {
	s, _ := newStore(t)
	got, err := s.InsertPending(context.Background(), append(rows("k1"), rows("k1")...))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("inserted %d rows, want 2", len(got))
	}
}

func TestInsertPendingEmptyBatch(t *testing.T) {
	s, _ := newStore(t)
	got, err := s.InsertPending(context.Background(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestMarkAcceptedAndFailedOnlyMovePendingRows(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()
	got, err := s.InsertPending(ctx, rows("k1"))
	if err != nil {
		t.Fatal(err)
	}
	email, sms := got[0].ID, got[1].ID

	if err := s.MarkAccepted(ctx, email, "provider-a", "msg-1"); err != nil {
		t.Fatal(err)
	}
	if state, _, provider := stateOf(t, conn, email); state != "accepted" || provider == nil || *provider != "provider-a" {
		t.Errorf("email row = %s %v", state, provider)
	}
	var acceptedAt *time.Time
	if err := conn.QueryRow(ctx, `SELECT accepted_at FROM notifications WHERE id = $1`, email).Scan(&acceptedAt); err != nil || acceptedAt == nil {
		t.Errorf("accepted_at = %v, %v", acceptedAt, err)
	}

	if err := s.MarkFailed(ctx, sms, "rejected"); err != nil {
		t.Fatal(err)
	}
	if state, reason, _ := stateOf(t, conn, sms); state != "failed" || reason == nil || *reason != "rejected" {
		t.Errorf("sms row = %s %v", state, reason)
	}

	// A late outcome must not move a row backward or sideways.
	if err := s.MarkFailed(ctx, email, "late"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAccepted(ctx, sms, "provider-b", "msg-2"); err != nil {
		t.Fatal(err)
	}
	if state, _, _ := stateOf(t, conn, email); state != "accepted" {
		t.Errorf("accepted row became %s", state)
	}
	if state, _, provider := stateOf(t, conn, sms); state != "failed" || provider != nil {
		t.Errorf("failed row became %s with provider %v", state, provider)
	}
}

func TestMarkUnresolvedOnlyMovesPendingRows(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()
	got, err := s.InsertPending(ctx, rows("k1"))
	if err != nil {
		t.Fatal(err)
	}
	email, sms := got[0].ID, got[1].ID

	if err := s.MarkUnresolved(ctx, email, "both providers timed out"); err != nil {
		t.Fatal(err)
	}
	if state, reason, _ := stateOf(t, conn, email); state != "unresolved" || reason == nil || *reason != "both providers timed out" {
		t.Errorf("email row = %s %v", state, reason)
	}

	if err := s.MarkAccepted(ctx, sms, "provider-a", "msg-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUnresolved(ctx, sms, "late"); err != nil {
		t.Fatal(err)
	}
	if state, _, _ := stateOf(t, conn, sms); state != "accepted" {
		t.Errorf("accepted row became %s", state)
	}

	// A failed row keeps its state and reason.
	got, err = s.InsertPending(ctx, rows("k2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkFailed(ctx, got[0].ID, "rejected"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUnresolved(ctx, got[0].ID, "late"); err != nil {
		t.Fatal(err)
	}
	if state, reason, _ := stateOf(t, conn, got[0].ID); state != "failed" || reason == nil || *reason != "rejected" {
		t.Errorf("failed row became %s %v", state, reason)
	}
}
