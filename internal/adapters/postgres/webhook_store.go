package postgres

import (
	"context"
	"fmt"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

var _ ports.WebhookStore = (*Store)(nil)

// The two statements below share their middle. Each starts from `src`, one row
// per event with the notification it resolves to (nid) and the status, reason
// and idempotency key read from the payload, which is the only stored copy.
// They differ in how events enter (a batch from the endpoint, or the saved
// events with no notification yet) and in how they are saved or linked.
//
// Resolution (resolveNotification) prefers the provider's message id: the row
// that provider accepted, whatever its state. Only if no row holds that id does
// it fall back to the idempotency key and channel, and then only for an
// `unresolved` row with no message id, which is a send that timed out before
// any provider named an id. A row that was accepted by one provider is never
// matched by key, so a late event from a provider it fell over from cannot
// overwrite the outcome of the provider that took it. A `pending` row is not
// matched at all: a webhook that beats the accept stays unlinked, and
// ResolvePending links it once the row is accepted.
//
// applyStates moves each linked notification to its event's final state, only
// from `accepted` or `unresolved` (the transitions domain.CanTransition allows
// to delivered and failed), so a final state stays final and a duplicate is a
// no-op. When several events of a batch hit one notification, the first in
// order wins. On a row that had no provider, it records the provider and
// message id of the event, which is the first proof of who took the message;
// row_number keeps a malformed provider that reuses a message id for two rows
// from violating the unique (provider, message id) index: every row takes its
// state, but only the first records the id.
//
// Only an accept gives a row a provider message id, together with its state
// (MarkAccepted), so a `pending` row never holds one and an event matched by
// message id is always on a row that can take its state.
//
// An event matched by key may carry a message id that another row already
// holds. The unique index spans (provider, provider_message_id) and not the
// channel, while the lookup is per channel, so the id is recorded only if no
// row holds it; otherwise the row still takes the state, without the id.
//
// The reason is cut to 500 characters here, as the sender cuts its own, since
// this statement is what writes failure_reason.
//
// Two webhook processes touching the same rows can deadlock, or collide on that
// index; either fails the statement, the endpoint answers 503 and the provider
// retries, and the retry finds the settled rows.
const (
	// resolveNotification is the id of the notification event %[1]s belongs to,
	// or NULL.
	resolveNotification = `COALESCE(
    (SELECT n.id FROM notifications n
      WHERE n.provider = %[1]s.provider AND n.provider_message_id = %[1]s.mid AND n.channel = %[1]s.channel),
    (SELECT n.id FROM notifications n
      WHERE n.idempotency_key = %[1]s.key AND n.channel = %[1]s.channel
        AND n.provider_message_id IS NULL AND n.state = 'unresolved'))`

	applyStates = `
upd AS (
    UPDATE notifications n
    SET state = a.status,
        failure_reason = CASE WHEN a.status = 'failed' THEN COALESCE(NULLIF(a.reason, ''), 'provider reported failure') END,
        provider = CASE WHEN n.provider_message_id IS NULL AND a.id_free THEN a.provider ELSE n.provider END,
        provider_message_id = CASE WHEN n.provider_message_id IS NULL AND a.id_free THEN a.mid ELSE n.provider_message_id END,
        updated_at = now()
    FROM (
        SELECT f.*,
               NOT EXISTS (SELECT 1 FROM notifications x WHERE x.provider = f.provider AND x.provider_message_id = f.mid)
                   AND row_number() OVER (PARTITION BY f.provider, f.mid ORDER BY f.ord) = 1 AS id_free
        FROM (SELECT DISTINCT ON (nid) * FROM todo ORDER BY nid, ord) f
    ) a
    WHERE n.id = a.nid AND n.state IN ('accepted', 'unresolved')
    RETURNING n.id
)`
)

// recordEvents saves a batch of events and applies their states in one
// statement. The ids are made here so the inserted rows can be matched back to
// `src`; an event skipped by the (provider, event_id) constraint is missing from
// `ins`, so its state is not applied a second time. An empty event id becomes
// NULL, which the constraint never treats as a duplicate.
var recordEvents = `
WITH ev AS MATERIALIZED (
    SELECT gen_random_uuid() AS id, u.ord, u.provider, u.channel, NULLIF(u.event_id, '') AS event_id, u.mid,
           p.j AS payload, p.j->>'idempotency_key' AS key, p.j->>'status' AS status, left(p.j->>'reason', 500) AS reason
    FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
        WITH ORDINALITY AS u(provider, channel, event_id, mid, payload, ord)
    CROSS JOIN LATERAL (SELECT u.payload::jsonb AS j) p
),
src AS MATERIALIZED (
    SELECT ev.*, ` + fmt.Sprintf(resolveNotification, "ev") + ` AS nid
    FROM ev
),
ins AS (
    INSERT INTO webhook_events (id, provider, channel, event_id, provider_message_id, notification_id, payload)
    SELECT id, provider, channel, event_id, mid, nid, payload FROM src ORDER BY ord
    ON CONFLICT ON CONSTRAINT webhook_events_provider_event_key DO NOTHING
    RETURNING id
),
todo AS (
    SELECT s.* FROM src s JOIN ins ON ins.id = s.id
    WHERE s.nid IS NOT NULL AND s.status IN ('delivered', 'failed')
),` + applyStates + `
SELECT count(*) FROM upd`

// RecordEvents implements ports.WebhookStore.
func (s *Store) RecordEvents(ctx context.Context, events []domain.WebhookEvent) error {
	if len(events) == 0 {
		return nil
	}
	var providers, channels, eventIDs, messageIDs, payloads []string
	for _, e := range events {
		if e.Status != domain.StateDelivered && e.Status != domain.StateFailed {
			return fmt.Errorf("record webhook event: status %q is not delivered or failed", e.Status)
		}
		providers = append(providers, e.Provider)
		channels = append(channels, string(e.Channel))
		eventIDs = append(eventIDs, e.EventID)
		messageIDs = append(messageIDs, e.ProviderMessageID)
		payloads = append(payloads, string(e.Payload))
	}
	if _, err := s.pool.Exec(ctx, recordEvents, providers, channels, eventIDs, messageIDs, payloads); err != nil {
		return fmt.Errorf("record %d webhook events: %w", len(events), err)
	}
	return nil
}

// resolveWindow is how far back ResolvePending looks. An event that is still
// unlinked after this long is not going to be (the sender and janitors settle a
// row within minutes), so it stays stored for inspection and stops costing a
// lookup on every pass. The index on created_at where notification_id is null
// serves the range.
const resolveWindow = "1 hour"

// resolvePending links saved events that have no notification, oldest first,
// and applies their states. Only events that resolve now count against the
// limit, so events that cannot resolve yet (a row still pending, or never
// seen) never hold up the ones behind them. Two processes may pick the same
// event: linking is guarded by `notification_id IS NULL` and applying by the
// row's state, so the second does nothing.
var resolvePending = `
WITH ev AS MATERIALIZED (
    SELECT e.id, e.created_at, e.provider, e.channel, e.provider_message_id AS mid, e.payload,
           e.payload->>'idempotency_key' AS key, e.payload->>'status' AS status, left(e.payload->>'reason', 500) AS reason
    FROM webhook_events e
    WHERE e.notification_id IS NULL AND e.created_at > now() - interval '` + resolveWindow + `'
),
src AS MATERIALIZED (
    SELECT l.*, row_number() OVER (ORDER BY l.created_at, l.id) AS ord FROM (
        SELECT t.* FROM (SELECT ev.*, ` + fmt.Sprintf(resolveNotification, "ev") + ` AS nid FROM ev) t
        WHERE t.nid IS NOT NULL
        ORDER BY t.created_at, t.id
        LIMIT $1
    ) l
),
linked AS (
    UPDATE webhook_events e SET notification_id = s.nid
    FROM src s
    WHERE e.id = s.id AND e.notification_id IS NULL
    RETURNING e.id
),
todo AS (
    SELECT s.* FROM src s JOIN linked l ON l.id = s.id
    WHERE s.status IN ('delivered', 'failed')
),` + applyStates + `
SELECT count(*) FROM linked`

// ResolvePending implements ports.WebhookStore.
func (s *Store) ResolvePending(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("resolve pending webhook events: limit must be at least 1, got %d", limit)
	}
	var linked int
	if err := s.pool.QueryRow(ctx, resolvePending, limit).Scan(&linked); err != nil {
		return 0, fmt.Errorf("resolve pending webhook events: %w", err)
	}
	return linked, nil
}
