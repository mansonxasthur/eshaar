package domain

import (
	"errors"
	"fmt"
)

// State is the delivery state of one notification row.
type State string

// The delivery states.
const (
	StatePending    State = "pending"
	StateAccepted   State = "accepted"
	StateDelivered  State = "delivered"
	StateFailed     State = "failed"
	StateUnresolved State = "unresolved"
)

var (
	// ErrInvalidTransition is returned when a state change would move backward
	// or is not part of the delivery flow.
	ErrInvalidTransition = errors.New("invalid state transition")

	// ErrNoChange is returned when the row is already in the requested state,
	// as with a duplicate webhook. Callers treat it as an idempotent no-op,
	// not a failure.
	ErrNoChange = errors.New("already in requested state")
)

// transitions lists every allowed move. Anything absent is rejected, which
// keeps the machine forward-only: delivered and failed are terminal, and
// unresolved can only be resolved by a late webhook.
//
// pending may become failed when every provider rejects the send outright
// (as opposed to timing out, which ends in unresolved). pending cannot jump to
// delivered: a webhook that beats the accept is stored unresolved in
// webhook_events and re-resolved by the webhook phase once the row is
// accepted. accepted cannot become unresolved: only pending rows are given up
// on, by the recovery janitor and the sweeper.
var transitions = map[State][]State{
	StatePending:    {StateAccepted, StateFailed, StateUnresolved},
	StateAccepted:   {StateDelivered, StateFailed},
	StateUnresolved: {StateDelivered, StateFailed},
}

// Valid reports whether s is one of the known states.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateAccepted, StateDelivered, StateFailed, StateUnresolved:
		return true
	}
	return false
}

// Final reports whether s is delivered or failed.
func (s State) Final() bool {
	return s == StateDelivered || s == StateFailed
}

// CanTransition reports whether a row in state from may move to state to.
func CanTransition(from, to State) bool {
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

func checkTransition(from, to State) error {
	if from == to {
		return fmt.Errorf("%w: %s", ErrNoChange, to)
	}
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}
