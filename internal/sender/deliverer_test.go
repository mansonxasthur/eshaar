package sender

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

func newDeliverer(t *testing.T, routes ...Route) (*Deliverer, *fakeStore) {
	t.Helper()
	store := newStore(&recorder{})
	d, err := NewDeliverer(store, map[domain.Channel][]Route{domain.ChannelSMS: routes},
		DelivererConfig{SendTimeout: time.Second, StatusTimeout: 100 * time.Millisecond}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return d, store
}

func pendingRow(last string) domain.Notification {
	n := domain.Notification{ID: "id1", IdempotencyKey: "k1", Channel: domain.ChannelSMS, State: domain.StatePending}
	if last != "" {
		n.LastProvider = &last
	}
	return n
}

func route(p *stub) Route { return Route{Provider: p, Timeout: 500 * time.Millisecond} }

func TestResumeStartsAtTheLastProvider(t *testing.T) {
	a := &stub{name: "a", send: accepts("msg-a")}
	b := &stub{name: "b", send: accepts("msg-b")}
	d, store := newDeliverer(t, route(a), route(b))

	d.Resume(context.Background(), pendingRow("b"))

	if a.calls.Load() != 0 || b.calls.Load() != 1 {
		t.Errorf("calls a=%d b=%d, want a skipped and b called once", a.calls.Load(), b.calls.Load())
	}
	if got := store.accepted["id1"]; got != [2]string{"b", "msg-b"} {
		t.Errorf("accepted = %v", got)
	}
}

func TestResumeEndsByTryingTheProvidersBeforeTheResumePoint(t *testing.T) {
	a := &stub{name: "a", send: accepts("msg-a")} // skipped by its breaker on the first run
	b := &stub{name: "b", send: fails(errRejected)}
	d, store := newDeliverer(t, route(a), route(b))

	d.Resume(context.Background(), pendingRow("b"))

	if a.calls.Load() != 1 || b.calls.Load() != 1 {
		t.Errorf("calls a=%d b=%d, want both tried once", a.calls.Load(), b.calls.Load())
	}
	if got := store.accepted["id1"]; got != [2]string{"a", "msg-a"} {
		t.Errorf("accepted = %v", got)
	}
	if len(store.failed) != 0 {
		t.Error("a row some provider could still take was failed")
	}
}

func TestResumeFailsOnlyAfterEveryProviderRefused(t *testing.T) {
	a := &stub{name: "a", send: fails(errRejected)}
	b := &stub{name: "b", send: fails(errRejected)}
	d, store := newDeliverer(t, route(a), route(b))

	d.Resume(context.Background(), pendingRow("b"))

	if a.calls.Load() != 1 || b.calls.Load() != 1 {
		t.Errorf("calls a=%d b=%d, want both tried once", a.calls.Load(), b.calls.Load())
	}
	if _, ok := store.failed["id1"]; !ok {
		t.Error("row not failed")
	}
}

func TestResumeFallsThroughToLaterProviders(t *testing.T) {
	a := &stub{name: "a", send: fails(errRejected)}
	b := &stub{name: "b", send: fails(errDown)}
	c := &stub{name: "c", send: accepts("msg-c")}
	d, store := newDeliverer(t, route(a), route(b), route(c))

	d.Resume(context.Background(), pendingRow("b"))

	if a.calls.Load() != 0 || b.calls.Load() != 1 || c.calls.Load() != 1 {
		t.Errorf("calls a=%d b=%d c=%d", a.calls.Load(), b.calls.Load(), c.calls.Load())
	}
	if got := store.accepted["id1"]; got != [2]string{"c", "msg-c"} {
		t.Errorf("accepted = %v", got)
	}
}

func TestResumeAcceptedByStatusCheckSendsNothing(t *testing.T) {
	a := &checking{
		stub:   &stub{name: "a", send: accepts("never")},
		status: func(context.Context, string, domain.Channel) (string, bool, error) { return "msg-a", true, nil },
	}
	b := &stub{name: "b", send: accepts("never")}
	d, store := newDeliverer(t, Route{Provider: a, Timeout: time.Second}, route(b))

	d.Resume(context.Background(), pendingRow("a"))

	if a.calls.Load() != 0 || b.calls.Load() != 0 {
		t.Errorf("sent a=%d b=%d, want no send", a.calls.Load(), b.calls.Load())
	}
	if got := store.accepted["id1"]; got != [2]string{"a", "msg-a"} {
		t.Errorf("accepted = %v", got)
	}
}

func TestResumeSendsWhenTheStatusCheckFindsNothing(t *testing.T) {
	a := &checking{
		stub:   &stub{name: "a", send: accepts("msg-a")},
		status: func(context.Context, string, domain.Channel) (string, bool, error) { return "", false, nil },
	}
	d, store := newDeliverer(t, Route{Provider: a, Timeout: time.Second})

	d.Resume(context.Background(), pendingRow("a"))

	if a.checks.Load() != 1 || a.calls.Load() != 1 {
		t.Errorf("checks %d sends %d, want 1 and 1", a.checks.Load(), a.calls.Load())
	}
	if got := store.accepted["id1"]; got != [2]string{"a", "msg-a"} {
		t.Errorf("accepted = %v", got)
	}
}

func TestResumeWithoutALastProviderStartsAtTheFirst(t *testing.T) {
	for name, last := range map[string]string{"empty": "", "gone from the chain": "retired"} {
		t.Run(name, func(t *testing.T) {
			a := &checking{
				stub:   &stub{name: "a", send: accepts("msg-a")},
				status: func(context.Context, string, domain.Channel) (string, bool, error) { return "", false, nil },
			}
			b := &stub{name: "b", send: accepts("msg-b")}
			d, store := newDeliverer(t, Route{Provider: a, Timeout: time.Second}, route(b))

			d.Resume(context.Background(), pendingRow(last))

			// A crash after the send can leave no last provider, so the first
			// provider is asked before it is called again.
			if a.checks.Load() != 1 || a.calls.Load() != 1 || b.calls.Load() != 0 {
				t.Errorf("a checks %d sends %d, b sends %d", a.checks.Load(), a.calls.Load(), b.calls.Load())
			}
			if got := store.accepted["id1"]; got != [2]string{"a", "msg-a"} {
				t.Errorf("accepted = %v", got)
			}
		})
	}
}

func TestResumeThatEndsPendingRecordsTheLastProviderAgain(t *testing.T) {
	a := &stub{name: "a", send: fails(errRejected)}
	b := &stub{name: "b", send: fails(ErrCircuitOpen)}
	d, store := newDeliverer(t, route(a), route(b))

	d.Resume(context.Background(), pendingRow("a"))

	if got := store.lastTried["id1"]; got != "a" {
		t.Errorf("last provider = %q, want a", got)
	}
	if len(store.accepted)+len(store.failed)+len(store.unresolved) != 0 {
		t.Error("a pending row was given an outcome")
	}
}

func TestResumeUsesTheFailureRules(t *testing.T) {
	a := &stub{name: "a", send: fails(errRejected)}
	d, store := newDeliverer(t, route(a))

	d.Resume(context.Background(), pendingRow("a"))

	if _, ok := store.failed["id1"]; !ok {
		t.Errorf("rejected by every remaining provider should fail the row: %+v", store)
	}
}

func TestResumeOnAChannelWithoutProvidersFails(t *testing.T) {
	d, store := newDeliverer(t)
	d.Resume(context.Background(), pendingRow("a"))
	if _, ok := store.failed["id1"]; !ok {
		t.Error("a channel with no providers should fail the row")
	}
}
