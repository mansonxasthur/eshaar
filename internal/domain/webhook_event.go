package domain

import (
	"encoding/json"
	"time"
)

// WebhookEvent is a provider callback, stored as received. EventID is empty
// when the provider sends none; the store keeps it NULL then, so such events
// never collide on the (provider, event id) unique constraint. NotificationID is
// empty until the event is resolved to a notification.
//
// IdempotencyKey, Status and Reason come from the payload, which is the only
// copy stored: they are the fields the store needs to find the notification and
// to apply the final state. Status is StateDelivered or StateFailed.
type WebhookEvent struct {
	ID                string
	Provider          string
	Channel           Channel
	EventID           string
	ProviderMessageID string
	NotificationID    string
	IdempotencyKey    string
	Status            State
	Reason            string
	Payload           json.RawMessage
	CreatedAt         time.Time
}
