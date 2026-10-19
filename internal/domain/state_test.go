package domain

import (
	"errors"
	"testing"
	"time"
)

var allStates = []State{StatePending, StateAccepted, StateDelivered, StateFailed, StateUnresolved}

func TestCanTransition(t *testing.T) {
	allowed := map[[2]State]bool{
		{StatePending, StateAccepted}:     true,
		{StatePending, StateFailed}:       true,
		{StatePending, StateUnresolved}:   true,
		{StateAccepted, StateDelivered}:   true,
		{StateAccepted, StateFailed}:      true,
		{StateUnresolved, StateDelivered}: true,
		{StateUnresolved, StateFailed}:    true,
	}
	for _, from := range allStates {
		for _, to := range allStates {
			if got, want := CanTransition(from, to), allowed[[2]State{from, to}]; got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestFinalStatesAreTerminal(t *testing.T) {
	for _, from := range []State{StateDelivered, StateFailed} {
		if !from.Final() {
			t.Errorf("%s should be final", from)
		}
		for _, to := range allStates {
			if CanTransition(from, to) {
				t.Errorf("final state %s must not move to %s", from, to)
			}
		}
	}
}

func TestTransition(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	n := &Notification{State: StatePending}
	if err := n.Transition(StateAccepted, now); err != nil {
		t.Fatalf("pending -> accepted: %v", err)
	}
	if n.State != StateAccepted || n.AcceptedAt == nil || !n.AcceptedAt.Equal(now) || !n.UpdatedAt.Equal(now) {
		t.Errorf("accept not recorded: %+v", n)
	}

	later := now.Add(time.Minute)
	err := n.Transition(StatePending, later)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("accepted -> pending: err = %v, want ErrInvalidTransition", err)
	}
	if n.State != StateAccepted || !n.UpdatedAt.Equal(now) {
		t.Errorf("rejected transition changed the notification: %+v", n)
	}
}

func TestUnresolvedResolvedByLateWebhook(t *testing.T) {
	n := &Notification{State: StatePending}
	now := time.Now()
	for _, to := range []State{StateUnresolved, StateDelivered} {
		if err := n.Transition(to, now); err != nil {
			t.Fatalf("-> %s: %v", to, err)
		}
	}
}

func TestStateValid(t *testing.T) {
	for _, s := range allStates {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	if State("bogus").Valid() {
		t.Error("bogus should be invalid")
	}
}

func TestParseChannel(t *testing.T) {
	for _, c := range Channels() {
		if got, err := ParseChannel(string(c)); err != nil || got != c {
			t.Errorf("ParseChannel(%q) = %q, %v", c, got, err)
		}
	}
	if _, err := ParseChannel("fax"); err == nil {
		t.Error("ParseChannel(fax) should fail")
	}
}

func TestSameStateIsNoChange(t *testing.T) {
	now := time.Now()
	for _, s := range allStates {
		n := &Notification{State: s}
		if err := n.Transition(s, now); !errors.Is(err, ErrNoChange) {
			t.Errorf("%s -> %s: err = %v, want ErrNoChange", s, s, err)
		}
		if !n.UpdatedAt.IsZero() {
			t.Errorf("%s: no-op changed UpdatedAt", s)
		}
	}
}

func TestFinalRowRejectsFurtherTransitions(t *testing.T) {
	now := time.Now()
	n := &Notification{State: StateAccepted}
	if err := n.Transition(StateFailed, now); err != nil {
		t.Fatal(err)
	}
	if err := n.Transition(StateDelivered, now); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("failed -> delivered: err = %v, want ErrInvalidTransition", err)
	}
	if n.State != StateFailed {
		t.Errorf("state = %s, want failed", n.State)
	}
}

func TestAcceptAndFailRecordDetails(t *testing.T) {
	now := time.Now()

	n := &Notification{State: StatePending}
	if err := n.Accept("a", "", now); err == nil {
		t.Error("Accept with empty id should fail")
	}
	if err := n.Accept("provider-a", "msg-1", now); err != nil {
		t.Fatal(err)
	}
	if n.Provider == nil || *n.Provider != "provider-a" {
		t.Errorf("Provider = %v", n.Provider)
	}
	if n.ProviderMessageID == nil || *n.ProviderMessageID != "msg-1" {
		t.Errorf("ProviderMessageID = %v", n.ProviderMessageID)
	}

	f := &Notification{State: StatePending}
	if err := f.Fail("", now); err == nil {
		t.Error("Fail with empty reason should fail")
	}
	if err := f.Fail("rejected by both providers", now); err != nil {
		t.Fatal(err)
	}
	if f.FailureReason == nil || *f.FailureReason != "rejected by both providers" {
		t.Errorf("FailureReason = %v", f.FailureReason)
	}
}
