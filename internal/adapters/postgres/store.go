package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Store saves notifications in Postgres.
type Store struct {
	pool *pgxpool.Pool
}

var _ ports.NotificationStore = (*Store)(nil)

// NewStore connects a connection pool to databaseURL.
func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connections.
func (s *Store) Close() { s.pool.Close() }

// insertPending saves a batch with one statement. unnest turns the column
// arrays back into rows, so the statement text does not grow with the batch.
// A row that hits the (idempotency_key, channel) constraint is skipped and so
// is missing from RETURNING, which is how the caller tells new rows from
// rows saved by an earlier delivery. The same pair twice in one batch inserts
// once.
const insertPending = `
INSERT INTO notifications
    (idempotency_key, recipient_id, notification_type, channel, content, email, phone, state, created_at, updated_at)
SELECT u.key, u.recipient_id, u.notification_type, u.channel, u.content::jsonb, u.email, u.phone, 'pending', u.created_at, u.created_at
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::timestamptz[])
    AS u(key, recipient_id, notification_type, channel, content, email, phone, created_at)
ON CONFLICT ON CONSTRAINT notifications_idempotency_channel_key DO NOTHING
RETURNING id, idempotency_key, channel`

// InsertPending implements ports.NotificationStore.
func (s *Store) InsertPending(ctx context.Context, rows []domain.Notification) ([]domain.Notification, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	var (
		keys, recipients, types, channels, contents []string
		emails, phones                              []*string
		created                                     []time.Time
	)
	byKey := make(map[rowKey]domain.Notification, len(rows))
	for _, n := range rows {
		keys = append(keys, n.IdempotencyKey)
		recipients = append(recipients, n.RecipientID)
		types = append(types, n.NotificationType)
		channels = append(channels, string(n.Channel))
		contents = append(contents, string(n.Content))
		emails = append(emails, n.Email)
		phones = append(phones, n.Phone)
		created = append(created, n.CreatedAt)
		byKey[rowKey{n.IdempotencyKey, n.Channel}] = n
	}

	res, err := s.pool.Query(ctx, insertPending, keys, recipients, types, channels, contents, emails, phones, created)
	if err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}
	defer res.Close()

	var inserted []domain.Notification
	for res.Next() {
		var (
			id, key, channel string
		)
		if err := res.Scan(&id, &key, &channel); err != nil {
			return nil, fmt.Errorf("scan inserted row: %w", err)
		}
		n := byKey[rowKey{key, domain.Channel(channel)}]
		n.ID = id
		inserted = append(inserted, n)
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}
	return inserted, nil
}

type rowKey struct {
	idempotencyKey string
	channel        domain.Channel
}

// MarkAccepted implements ports.NotificationStore. The state guard makes a
// late call a no-op instead of moving a row backward.
func (s *Store) MarkAccepted(ctx context.Context, id, provider, providerMessageID string) error {
	_, err := s.pool.Exec(ctx, `
UPDATE notifications
SET state = 'accepted', provider = $2, provider_message_id = $3, accepted_at = now(), updated_at = now()
WHERE id = $1 AND state = 'pending'`, id, provider, providerMessageID)
	if err != nil {
		return fmt.Errorf("mark %s accepted: %w", id, err)
	}
	return nil
}

// MarkFailed implements ports.NotificationStore.
func (s *Store) MarkFailed(ctx context.Context, id, reason string) error {
	return s.markPending(ctx, id, "failed", reason)
}

// MarkUnresolved implements ports.NotificationStore.
func (s *Store) MarkUnresolved(ctx context.Context, id, reason string) error {
	return s.markPending(ctx, id, "unresolved", reason)
}

// markPending moves a pending row to a state that carries a reason. The state
// guard makes a late call a no-op.
func (s *Store) markPending(ctx context.Context, id, state, reason string) error {
	_, err := s.pool.Exec(ctx, `
UPDATE notifications
SET state = $2, failure_reason = $3, updated_at = now()
WHERE id = $1 AND state = 'pending'`, id, state, reason)
	if err != nil {
		return fmt.Errorf("mark %s %s: %w", id, state, err)
	}
	return nil
}
