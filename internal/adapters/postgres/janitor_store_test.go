package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const staleAfter = 10 * time.Second

// seed inserts one pending email row and returns its id. created and claimed
// are ages in the past; a zero claimed leaves claimed_at null.
func seed(t *testing.T, conn *pgx.Conn, key string, created, claimed time.Duration, attempts int) string {
	t.Helper()
	var claimedAt any
	if claimed > 0 {
		claimedAt = time.Now().Add(-claimed)
	}
	var id string
	err := conn.QueryRow(context.Background(), `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, state, created_at, claimed_at, attempts)
VALUES ($1, 'u1', 'otp', 'email', '{"template_id":"t"}', 'a@example.com', 'pending', $2, $3, $4)
RETURNING id`, key, time.Now().Add(-created), claimedAt, attempts).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func claimedIDs(t *testing.T, s *Store, limit int) []string {
	t.Helper()
	got, err := s.ClaimStale(context.Background(), staleAfter, limit)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, n := range got {
		ids = append(ids, n.ID)
	}
	slices.Sort(ids)
	return ids
}

func TestClaimStaleSelectionRules(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()

	oldUnclaimed := seed(t, conn, "old-unclaimed", time.Minute, 0, 0)
	seed(t, conn, "young-unclaimed", time.Second, 0, 0)
	oldClaimed := seed(t, conn, "old-claimed", time.Minute, 30*time.Second, 1)
	seed(t, conn, "young-claimed", time.Minute, 2*time.Second, 1)
	seed(t, conn, "exhausted", time.Minute, time.Minute, maxAttempts)
	done := seed(t, conn, "accepted", time.Minute, 0, 0)
	if _, err := conn.Exec(ctx, `UPDATE notifications SET state = 'failed' WHERE id = $1`, done); err != nil {
		t.Fatal(err)
	}

	got := claimedIDs(t, s, 100)
	want := []string{oldUnclaimed, oldClaimed}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("claimed %v, want %v", got, want)
	}

	var attempts int
	var claimedAt *time.Time
	err := conn.QueryRow(ctx, `SELECT attempts, claimed_at FROM notifications WHERE id = $1`, oldUnclaimed).Scan(&attempts, &claimedAt)
	if err != nil || attempts != 1 || claimedAt == nil || time.Since(*claimedAt) > time.Minute {
		t.Errorf("unclaimed row after claim: attempts %d claimed_at %v err %v", attempts, claimedAt, err)
	}
	if err := conn.QueryRow(ctx, `SELECT attempts FROM notifications WHERE id = $1`, oldClaimed).Scan(&attempts); err != nil || attempts != 2 {
		t.Errorf("claimed row attempts = %d, %v, want 2", attempts, err)
	}

	// Claiming moved claimed_at to now, so the rows are not stale again.
	if again := claimedIDs(t, s, 100); len(again) != 0 {
		t.Errorf("claimed %v twice in a row", again)
	}
}

func TestClaimStaleReturnsTheRowToResend(t *testing.T) {
	s, conn := newStore(t)
	id := seed(t, conn, "k1", time.Minute, 0, 0)
	if _, err := conn.Exec(context.Background(), `UPDATE notifications SET last_provider = 'email-a' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	got, err := s.ClaimStale(context.Background(), staleAfter, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v, %v", got, err)
	}
	n := got[0]
	if n.ID != id || n.IdempotencyKey != "k1" || n.Channel != "email" || n.Email == nil || *n.Email != "a@example.com" ||
		n.LastProvider == nil || *n.LastProvider != "email-a" || n.Attempts != 1 || len(n.Content) == 0 || n.State != "pending" {
		t.Errorf("claimed row = %+v", n)
	}
}

func TestClaimStaleHonoursLimitAndOrder(t *testing.T) {
	s, conn := newStore(t)
	oldest := seed(t, conn, "a", 3*time.Hour, 0, 0)
	next := seed(t, conn, "b", 2*time.Hour, 0, 0)
	seed(t, conn, "c", time.Hour, 0, 0)

	got := claimedIDs(t, s, 2)
	want := []string{oldest, next}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("claimed %v, want the two oldest %v", got, want)
	}
}

func TestSweepExhaustedOnlyTouchesExhaustedRows(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()

	exhausted := seed(t, conn, "exhausted", time.Hour, time.Minute, maxAttempts)
	inFlight := seed(t, conn, "last-redrive-running", time.Hour, 2*time.Second, maxAttempts)
	hasAttempts := seed(t, conn, "has-attempts", time.Hour, time.Minute, maxAttempts-1)

	n, err := s.SweepExhausted(ctx, staleAfter, 100)
	if err != nil || n != 1 {
		t.Fatalf("swept %d, %v, want 1", n, err)
	}
	if state, reason, _ := stateOf(t, conn, exhausted); state != "unresolved" || reason == nil || *reason == "" {
		t.Errorf("exhausted row = %s %v", state, reason)
	}
	for name, id := range map[string]string{"in flight": inFlight, "has attempts": hasAttempts} {
		if state, _, _ := stateOf(t, conn, id); state != "pending" {
			t.Errorf("%s row became %s", name, state)
		}
	}
}

func TestSweepExhaustedHonoursLimit(t *testing.T) {
	s, conn := newStore(t)
	for i := range 5 {
		seed(t, conn, fmt.Sprintf("k%d", i), time.Hour, time.Minute, maxAttempts)
	}
	for _, want := range []int{2, 2, 1, 0} {
		if n, err := s.SweepExhausted(context.Background(), staleAfter, 2); err != nil || n != want {
			t.Fatalf("swept %d, %v, want %d", n, err, want)
		}
	}
}

func TestTwoJanitorsNeverClaimTheSameRow(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()
	const total = 400
	if _, err := conn.Exec(ctx, `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, created_at)
SELECT 'k' || i, 'u1', 'otp', 'email', '{"template_id":"t"}', 'a@example.com', now() - interval '1 minute'
FROM generate_series(1, $1) AS i`, total); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for {
				got, err := s.ClaimStale(ctx, staleAfter, 7)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, n := range got {
					seen[n.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("claimed %d distinct rows, want %d", len(seen), total)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("row %s claimed %d times", id, c)
		}
	}
}

// The queries must use the partial indexes of migration 0005. On a table where
// nearly every row is long finished, a sequential scan would read all of them
// every 5 seconds, so a changed predicate or expression must fail here.
func TestJanitorQueriesUseTheirPartialIndexes(t *testing.T) {
	conn := newDB(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, state,
                           provider, provider_message_id, accepted_at, created_at)
SELECT 'done-' || i, 'u1', 'otp', 'email', '{"template_id":"t"}', 'a@example.com', 'accepted',
       'email-a', 'msg-' || i, now(), now() - interval '1 day'
FROM generate_series(1, 50000) AS i`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, created_at, claimed_at, attempts)
SELECT 'stuck-' || i, 'u1', 'otp', 'email', '{"template_id":"t"}', 'a@example.com',
       now() - interval '1 hour', CASE WHEN i % 2 = 0 THEN now() - interval '1 minute' END, 1
FROM generate_series(1, 100) AS i`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, email, created_at, claimed_at, attempts)
SELECT 'gone-' || i, 'u1', 'otp', 'email', '{"template_id":"t"}', 'a@example.com',
       now() - interval '1 hour', now() - interval '1 minute', $1
FROM generate_series(1, 100) AS i`, maxAttempts); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE notifications`); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct{ name, query, index string }{
		{"claim", claimStale, "notifications_recovery_pending_idx"},
		{"sweep", sweepExhausted, "notifications_exhausted_pending_idx"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			plan := explain(t, conn, tt.query, staleAfter.Seconds(), 100)
			if !strings.Contains(plan, tt.index) {
				t.Errorf("plan does not use %s:\n%s", tt.index, plan)
			}
			if strings.Contains(plan, "Seq Scan") {
				t.Errorf("plan has a sequential scan:\n%s", plan)
			}
		})
	}
}

func explain(t *testing.T, conn *pgx.Conn, query string, args ...any) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}
