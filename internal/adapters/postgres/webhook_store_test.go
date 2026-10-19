package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// seedRow inserts a notification in the given state straight into the table.
// provider and messageID are set only for rows that have them.
func seedRow(t *testing.T, conn *pgx.Conn, key string, channel domain.Channel, state, provider, messageID string) string {
	t.Helper()
	contact, column := "+966500000000", "phone"
	if channel == domain.ChannelEmail {
		contact, column = "a@example.com", "email"
	}
	var id string
	err := conn.QueryRow(context.Background(), fmt.Sprintf(`
INSERT INTO notifications (idempotency_key, recipient_id, notification_type, channel, content, %s, state, provider, provider_message_id, accepted_at)
VALUES ($1, 'u1', 'match_started', $2, '{}', $3, $4, NULLIF($5, ''), NULLIF($6, ''), CASE WHEN $4 = 'accepted' THEN now() END)
RETURNING id`, column), key, string(channel), contact, state, provider, messageID).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// event builds an event whose payload carries the same fields, as ParseEvent
// does, since the store reads the key, status and reason from the payload.
func event(provider string, channel domain.Channel, eventID, messageID, key string, status domain.State, reason string) domain.WebhookEvent {
	payload, err := json.Marshal(map[string]string{
		"event_id": eventID, "message_id": messageID, "idempotency_key": key,
		"channel": string(channel), "status": string(status), "reason": reason,
	})
	if err != nil {
		panic(err)
	}
	return domain.WebhookEvent{
		Provider: provider, Channel: channel, EventID: eventID, ProviderMessageID: messageID,
		IdempotencyKey: key, Status: status, Reason: reason, Payload: payload,
	}
}

type rowState struct {
	state                 string
	reason, provider, mid *string
}

func readRow(t *testing.T, conn *pgx.Conn, id string) rowState {
	t.Helper()
	var r rowState
	if err := conn.QueryRow(context.Background(),
		`SELECT state, failure_reason, provider, provider_message_id FROM notifications WHERE id = $1`, id).
		Scan(&r.state, &r.reason, &r.provider, &r.mid); err != nil {
		t.Fatal(err)
	}
	return r
}

func countEvents(t *testing.T, conn *pgx.Conn, where string, args ...any) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM webhook_events WHERE `+where, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventLink(t *testing.T, conn *pgx.Conn, provider, eventID string) *string {
	t.Helper()
	var link *string
	if err := conn.QueryRow(context.Background(),
		`SELECT notification_id FROM webhook_events WHERE provider = $1 AND event_id = $2`, provider, eventID).Scan(&link); err != nil {
		t.Fatal(err)
	}
	return link
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// TestWebhookStore shares one database between its subtests; every subtest uses
// its own idempotency keys, provider names and message ids.
func TestWebhookStore(t *testing.T) {
	s, conn := newStore(t)
	ctx := context.Background()
	record := func(t *testing.T, events ...domain.WebhookEvent) {
		t.Helper()
		if err := s.RecordEvents(ctx, events); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("delivered event moves an accepted row and links the event", func(t *testing.T) {
		id := seedRow(t, conn, "d1", domain.ChannelSMS, "accepted", "p-d1", "m1")
		record(t, event("p-d1", domain.ChannelSMS, "e1", "m1", "d1", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "delivered" || r.reason != nil {
			t.Errorf("row = %+v", r)
		}
		if l := eventLink(t, conn, "p-d1", "e1"); l == nil || *l != id {
			t.Errorf("event linked to %s, want %s", str(l), id)
		}
	})

	t.Run("failed event records the reason, Arabic intact", func(t *testing.T) {
		id := seedRow(t, conn, "f1", domain.ChannelSMS, "accepted", "p-f1", "m1")
		record(t, event("p-f1", domain.ChannelSMS, "e1", "m1", "f1", domain.StateFailed, "الرقم غير صالح"))
		if r := readRow(t, conn, id); r.state != "failed" || str(r.reason) != "الرقم غير صالح" {
			t.Errorf("row = %+v reason %s", r, str(r.reason))
		}
	})

	t.Run("failed event without a reason gets a default", func(t *testing.T) {
		id := seedRow(t, conn, "f2", domain.ChannelSMS, "accepted", "p-f2", "m1")
		record(t, event("p-f2", domain.ChannelSMS, "e1", "m1", "f2", domain.StateFailed, ""))
		if r := readRow(t, conn, id); r.state != "failed" || str(r.reason) != "provider reported failure" {
			t.Errorf("row = %+v reason %s", r, str(r.reason))
		}
	})

	t.Run("a duplicate event id is skipped and not applied again", func(t *testing.T) {
		id := seedRow(t, conn, "dup1", domain.ChannelSMS, "accepted", "p-dup1", "m1")
		record(t, event("p-dup1", domain.ChannelSMS, "e1", "m1", "dup1", domain.StateDelivered, ""))
		// The same id again, now claiming failure, must change nothing.
		record(t, event("p-dup1", domain.ChannelSMS, "e1", "m1", "dup1", domain.StateFailed, "late"))
		if r := readRow(t, conn, id); r.state != "delivered" {
			t.Errorf("state = %s, want delivered", r.state)
		}
		if n := countEvents(t, conn, `provider = 'p-dup1'`); n != 1 {
			t.Errorf("%d events stored, want 1", n)
		}
	})

	t.Run("the same event id from two providers is not a duplicate", func(t *testing.T) {
		a := seedRow(t, conn, "two1", domain.ChannelSMS, "accepted", "p-two-a", "m1")
		b := seedRow(t, conn, "two2", domain.ChannelEmail, "accepted", "p-two-b", "m1")
		record(t, event("p-two-a", domain.ChannelSMS, "same", "m1", "", domain.StateDelivered, ""),
			event("p-two-b", domain.ChannelEmail, "same", "m1", "", domain.StateFailed, "x"))
		if ra, rb := readRow(t, conn, a), readRow(t, conn, b); ra.state != "delivered" || rb.state != "failed" {
			t.Errorf("states = %s, %s", ra.state, rb.state)
		}
	})

	t.Run("the same message id under two providers resolves to each own row", func(t *testing.T) {
		a := seedRow(t, conn, "mid1", domain.ChannelSMS, "accepted", "p-mid-a", "shared")
		b := seedRow(t, conn, "mid2", domain.ChannelSMS, "accepted", "p-mid-b", "shared")
		record(t, event("p-mid-b", domain.ChannelSMS, "e1", "shared", "", domain.StateDelivered, ""))
		if ra, rb := readRow(t, conn, a), readRow(t, conn, b); ra.state != "accepted" || rb.state != "delivered" {
			t.Errorf("states = %s, %s", ra.state, rb.state)
		}
	})

	t.Run("events without an id are all stored, and applying twice is harmless", func(t *testing.T) {
		id := seedRow(t, conn, "nil1", domain.ChannelSMS, "accepted", "p-nil1", "m1")
		e := event("p-nil1", domain.ChannelSMS, "", "m1", "nil1", domain.StateDelivered, "")
		record(t, e)
		record(t, e, e)
		if n := countEvents(t, conn, `provider = 'p-nil1' AND event_id IS NULL`); n != 3 {
			t.Errorf("%d events stored with a NULL id, want 3", n)
		}
		if r := readRow(t, conn, id); r.state != "delivered" {
			t.Errorf("state = %s", r.state)
		}
	})

	t.Run("a final state stays final", func(t *testing.T) {
		id := seedRow(t, conn, "fin1", domain.ChannelSMS, "accepted", "p-fin1", "m1")
		record(t, event("p-fin1", domain.ChannelSMS, "e1", "m1", "fin1", domain.StateDelivered, ""))
		record(t, event("p-fin1", domain.ChannelSMS, "e2", "m1", "fin1", domain.StateFailed, "late failure"))
		if r := readRow(t, conn, id); r.state != "delivered" || r.reason != nil {
			t.Errorf("row = %+v", r)
		}
		if l := eventLink(t, conn, "p-fin1", "e2"); l == nil {
			t.Error("the late event should still be stored and linked")
		}
	})

	t.Run("the first of two events in one batch wins", func(t *testing.T) {
		id := seedRow(t, conn, "batch1", domain.ChannelSMS, "accepted", "p-batch1", "m1")
		record(t, event("p-batch1", domain.ChannelSMS, "e1", "m1", "batch1", domain.StateFailed, "first"),
			event("p-batch1", domain.ChannelSMS, "e2", "m1", "batch1", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "failed" || str(r.reason) != "first" {
			t.Errorf("row = %+v reason %s", r, str(r.reason))
		}
	})

	t.Run("a late event moves an unresolved row found by key, and fills its provider", func(t *testing.T) {
		id := seedRow(t, conn, "unres1", domain.ChannelEmail, "unresolved", "", "")
		record(t, event("p-unres1", domain.ChannelEmail, "e1", "late-m1", "unres1", domain.StateFailed, "bounced"))
		r := readRow(t, conn, id)
		if r.state != "failed" || str(r.provider) != "p-unres1" || str(r.mid) != "late-m1" || str(r.reason) != "bounced" {
			t.Errorf("row = %+v provider %s mid %s reason %s", r, str(r.provider), str(r.mid), str(r.reason))
		}
		if l := eventLink(t, conn, "p-unres1", "e1"); l == nil || *l != id {
			t.Errorf("event linked to %s, want %s", str(l), id)
		}
	})

	t.Run("delivered clears the reason an unresolved row carried", func(t *testing.T) {
		id := seedRow(t, conn, "clr1", domain.ChannelSMS, "unresolved", "", "")
		if _, err := conn.Exec(ctx, `UPDATE notifications SET failure_reason = 'all providers timed out' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		record(t, event("p-clr1", domain.ChannelSMS, "e1", "m1", "clr1", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "delivered" || r.reason != nil {
			t.Errorf("row = %+v reason %s", r, str(r.reason))
		}
	})

	t.Run("the reason is cut to 500 characters", func(t *testing.T) {
		id := seedRow(t, conn, "cut1", domain.ChannelSMS, "accepted", "p-cut1", "m1")
		record(t, event("p-cut1", domain.ChannelSMS, "e1", "m1", "cut1", domain.StateFailed, strings.Repeat("خطأ", 400)))
		r := readRow(t, conn, id)
		if got := []rune(str(r.reason)); len(got) != 500 {
			t.Errorf("reason has %d characters, want 500", len(got))
		}
	})

	t.Run("an id held by a row of another channel is not recorded twice", func(t *testing.T) {
		holder := seedRow(t, conn, "idh1", domain.ChannelSMS, "accepted", "p-idh", "m1")
		id := seedRow(t, conn, "idh2", domain.ChannelEmail, "unresolved", "", "")
		// Same provider and message id, other channel: the id lookup misses, the key
		// matches, and recording the id would break the unique index.
		record(t, event("p-idh", domain.ChannelEmail, "e1", "m1", "idh2", domain.StateDelivered, ""))
		r := readRow(t, conn, id)
		if r.state != "delivered" || r.mid != nil || r.provider != nil {
			t.Errorf("row = %+v provider %s mid %s", r, str(r.provider), str(r.mid))
		}
		if h := readRow(t, conn, holder); h.state != "accepted" {
			t.Errorf("holder = %+v", h)
		}
	})

	t.Run("an unresolved row is also found by message id", func(t *testing.T) {
		// An unresolved row whose provider did name an id, as after a status check.
		id := seedRow(t, conn, "unres2", domain.ChannelSMS, "unresolved", "p-unres2", "m1")
		record(t, event("p-unres2", domain.ChannelSMS, "e1", "m1", "", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "delivered" {
			t.Errorf("state = %s", r.state)
		}
	})

	t.Run("a row accepted by another provider is never matched by key", func(t *testing.T) {
		id := seedRow(t, conn, "other1", domain.ChannelSMS, "accepted", "sms-b", "mb1")
		// Provider A, which the send fell over from, reports a failure for the same key.
		record(t, event("p-other-a", domain.ChannelSMS, "e1", "ma1", "other1", domain.StateFailed, "a failed"))
		if r := readRow(t, conn, id); r.state != "accepted" || str(r.provider) != "sms-b" {
			t.Errorf("row = %+v provider %s", r, str(r.provider))
		}
		if l := eventLink(t, conn, "p-other-a", "e1"); l != nil {
			t.Errorf("event linked to %s, want it left unresolved", *l)
		}
	})

	t.Run("a channel mismatch does not match", func(t *testing.T) {
		id := seedRow(t, conn, "chan1", domain.ChannelSMS, "accepted", "p-chan1", "m1")
		record(t, event("p-chan1", domain.ChannelWhatsApp, "e1", "m1", "chan1", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "accepted" {
			t.Errorf("state = %s", r.state)
		}
	})

	t.Run("an event that beats the accept is stored, then resolved once accepted", func(t *testing.T) {
		id := seedRow(t, conn, "early1", domain.ChannelSMS, "pending", "", "")
		record(t, event("p-early1", domain.ChannelSMS, "e1", "m1", "early1", domain.StateFailed, "too fast"))
		if r := readRow(t, conn, id); r.state != "pending" {
			t.Fatalf("a pending row moved to %s", r.state)
		}
		if l := eventLink(t, conn, "p-early1", "e1"); l != nil {
			t.Fatalf("event linked to a pending row: %s", *l)
		}
		if _, err := s.ResolvePending(ctx, 100); err != nil {
			t.Fatal(err)
		}
		if got := readRow(t, conn, id); got.state != "pending" {
			t.Fatalf("resolve moved a pending row to %s", got.state)
		}

		if err := s.MarkAccepted(ctx, id, "p-early1", "m1"); err != nil {
			t.Fatal(err)
		}
		if n, err := s.ResolvePending(ctx, 100); err != nil || n < 1 {
			t.Fatalf("resolved %d, err %v", n, err)
		}
		if r := readRow(t, conn, id); r.state != "failed" || str(r.reason) != "too fast" {
			t.Errorf("row = %+v reason %s", r, str(r.reason))
		}
		if l := eventLink(t, conn, "p-early1", "e1"); l == nil || *l != id {
			t.Errorf("event linked to %s, want %s", str(l), id)
		}
		// Nothing left to do for it.
		if _, err := s.ResolvePending(ctx, 100); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("an unknown message id is stored unresolved without error", func(t *testing.T) {
		record(t, event("p-unknown", domain.ChannelSMS, "e1", "nobody", "no-such-key", domain.StateDelivered, ""))
		if l := eventLink(t, conn, "p-unknown", "e1"); l != nil {
			t.Errorf("linked to %s", *l)
		}
	})

	t.Run("resolving is not held up by older events that cannot resolve", func(t *testing.T) {
		record(t, event("p-stuck", domain.ChannelSMS, "old1", "ghost1", "", domain.StateDelivered, ""),
			event("p-stuck", domain.ChannelSMS, "old2", "ghost2", "", domain.StateDelivered, ""))
		id := seedRow(t, conn, "head1", domain.ChannelSMS, "pending", "", "")
		record(t, event("p-head1", domain.ChannelSMS, "e1", "m1", "head1", domain.StateDelivered, ""))
		if err := s.MarkAccepted(ctx, id, "p-head1", "m1"); err != nil {
			t.Fatal(err)
		}
		// Drain whatever other subtests left, then rely on a limit of 1 for this one.
		for {
			n, err := s.ResolvePending(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			if r := readRow(t, conn, id); r.state == "delivered" || n == 0 {
				break
			}
		}
		if r := readRow(t, conn, id); r.state != "delivered" {
			t.Errorf("state = %s: older unresolvable events blocked it", r.state)
		}
	})

	t.Run("an empty batch and a bad limit", func(t *testing.T) {
		if err := s.RecordEvents(ctx, nil); err != nil {
			t.Error(err)
		}
		if _, err := s.ResolvePending(ctx, 0); err == nil {
			t.Error("limit 0 accepted")
		}
	})

	t.Run("an event with a status other than delivered or failed is refused", func(t *testing.T) {
		e := event("p-bad", domain.ChannelSMS, "e1", "m1", "", domain.StateAccepted, "")
		if err := s.RecordEvents(ctx, []domain.WebhookEvent{e}); err == nil {
			t.Error("accepted status stored")
		}
	})

	t.Run("a pending row is never moved by key", func(t *testing.T) {
		id := seedRow(t, conn, "pk1", domain.ChannelSMS, "pending", "", "")
		record(t, event("p-pk1", domain.ChannelSMS, "e1", "m1", "pk1", domain.StateDelivered, ""))
		if r := readRow(t, conn, id); r.state != "pending" {
			t.Errorf("state = %s", r.state)
		}
	})

	t.Run("two rows reported with one reused message id both take the state, one records the id", func(t *testing.T) {
		a := seedRow(t, conn, "reuse1", domain.ChannelSMS, "unresolved", "", "")
		b := seedRow(t, conn, "reuse2", domain.ChannelSMS, "unresolved", "", "")
		record(t, event("p-reuse", domain.ChannelSMS, "e1", "same-id", "reuse1", domain.StateDelivered, ""),
			event("p-reuse", domain.ChannelSMS, "e2", "same-id", "reuse2", domain.StateDelivered, ""))
		ra, rb := readRow(t, conn, a), readRow(t, conn, b)
		if ra.state != "delivered" || rb.state != "delivered" {
			t.Errorf("states = %s, %s: both rows should take the state", ra.state, rb.state)
		}
		if (ra.mid != nil) == (rb.mid != nil) {
			t.Errorf("ids = %s, %s: exactly one row should record the id", str(ra.mid), str(rb.mid))
		}
	})

	t.Run("a batch mixes events for several rows", func(t *testing.T) {
		var ids []string
		var events []domain.WebhookEvent
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("many%d", i)
			ids = append(ids, seedRow(t, conn, key, domain.ChannelSMS, "accepted", "p-many", key))
			status := domain.StateDelivered
			if i%2 == 1 {
				status = domain.StateFailed
			}
			events = append(events, event("p-many", domain.ChannelSMS, "e"+key, key, key, status, "r"))
		}
		record(t, events...)
		for i, id := range ids {
			want := "delivered"
			if i%2 == 1 {
				want = "failed"
			}
			if r := readRow(t, conn, id); r.state != want {
				t.Errorf("row %d state = %s, want %s", i, r.state, want)
			}
		}
	})

	t.Run("concurrent batches with the same events settle on one result", func(t *testing.T) {
		id := seedRow(t, conn, "conc1", domain.ChannelSMS, "accepted", "p-conc1", "m1")
		e1 := event("p-conc1", domain.ChannelSMS, "e1", "m1", "conc1", domain.StateDelivered, "")
		e2 := event("p-conc1", domain.ChannelSMS, "e2", "m1", "conc1", domain.StateFailed, "x")
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				batch := []domain.WebhookEvent{e1, e2}
				if i%2 == 1 {
					batch = []domain.WebhookEvent{e2, e1}
				}
				errs <- s.RecordEvents(ctx, batch)
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("concurrent record: %v", err)
			}
		}
		r := readRow(t, conn, id)
		if r.state != "delivered" && r.state != "failed" {
			t.Errorf("state = %s", r.state)
		}
		if n := countEvents(t, conn, `provider = 'p-conc1'`); n != 2 {
			t.Errorf("%d events stored, want 2", n)
		}
	})
}
