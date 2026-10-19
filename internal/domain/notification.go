package domain

import (
	"encoding/json"
	"errors"
	"time"
)

// Notification is one channel's delivery of a request. A request that targets
// several channels becomes several rows sharing an idempotency key.
type Notification struct {
	ID                string
	IdempotencyKey    string
	RecipientID       string
	NotificationType  string
	Channel           Channel
	Content           json.RawMessage // only this channel's block, kept as UTF-8 JSON
	Email             *string         // set on email rows
	Phone             *string         // set on SMS and WhatsApp rows
	State             State
	FailureReason     *string
	Provider          *string // provider that accepted the send
	ProviderMessageID *string
	AcceptedAt        *time.Time
	ClaimedAt         *time.Time // set when the recovery janitor claims the row
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Transition moves the notification to state to at time now. It returns
// ErrNoChange if the row is already in that state and ErrInvalidTransition if
// the move is not allowed; either way the notification is left unchanged.
func (n *Notification) Transition(to State, now time.Time) error {
	if err := checkTransition(n.State, to); err != nil {
		return err
	}
	n.State = to
	n.UpdatedAt = now
	if to == StateAccepted {
		n.AcceptedAt = &now
	}
	return nil
}

// Accept records that a provider accepted the send and moves the row to
// accepted. The provider message ID is what webhooks use to find the row.
func (n *Notification) Accept(providerMessageID string, now time.Time) error {
	if providerMessageID == "" {
		return errors.New("accept: empty provider message id")
	}
	if err := n.Transition(StateAccepted, now); err != nil {
		return err
	}
	n.ProviderMessageID = &providerMessageID
	return nil
}

// Fail moves the row to failed and records why.
func (n *Notification) Fail(reason string, now time.Time) error {
	if reason == "" {
		return errors.New("fail: empty reason")
	}
	if err := n.Transition(StateFailed, now); err != nil {
		return err
	}
	n.FailureReason = &reason
	return nil
}
