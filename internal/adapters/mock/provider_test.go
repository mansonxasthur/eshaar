package mock

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

func TestSendReturnsDistinctMessageIDsAndRecords(t *testing.T) {
	p := NewProvider("mock")
	a, err := p.Send(context.Background(), domain.Notification{IdempotencyKey: "k1", Channel: domain.ChannelSMS})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := p.Send(context.Background(), domain.Notification{IdempotencyKey: "k2", Channel: domain.ChannelSMS})
	if a == b || a == "" {
		t.Errorf("message ids %q, %q", a, b)
	}
	if got := p.Sent(); len(got) != 2 || got[0].IdempotencyKey != "k1" {
		t.Errorf("sent = %+v", got)
	}
}

func TestSendFailsWithConfiguredError(t *testing.T) {
	boom := errors.New("boom")
	p := NewProvider("mock", WithError(boom))
	if _, err := p.Send(context.Background(), domain.Notification{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(p.Sent()) != 0 {
		t.Error("a failed send was recorded")
	}
}

func TestSendDelayEndsWithContext(t *testing.T) {
	p := NewProvider("mock", WithDelay(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.Send(ctx, domain.Notification{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

func TestSendDelayTimeoutIsErrTimeout(t *testing.T) {
	p := NewProvider("mock", WithDelay(time.Minute))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.Send(ctx, domain.Notification{}); !errors.Is(err, ports.ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}

func TestWithRateLimitedAnswersRateLimit(t *testing.T) {
	p := NewProvider("mock", WithRateLimited())
	if _, err := p.Send(context.Background(), domain.Notification{}); !errors.Is(err, ports.ErrRateLimited) {
		t.Fatalf("err = %v, want ErrRateLimited", err)
	}
}

func TestScriptRunsInOrderThenDefaults(t *testing.T) {
	boom := errors.New("boom")
	p := NewProvider("mock", WithScript(Step{Err: boom}, Step{}))
	ctx := context.Background()
	if _, err := p.Send(ctx, domain.Notification{}); !errors.Is(err, boom) {
		t.Fatalf("first call err = %v", err)
	}
	for i := range 2 {
		if _, err := p.Send(ctx, domain.Notification{}); err != nil {
			t.Fatalf("call %d err = %v", i+2, err)
		}
	}
	if len(p.Sent()) != 2 {
		t.Errorf("sent = %d, want 2", len(p.Sent()))
	}
}

func TestStatusFindsOnlyTakenMessages(t *testing.T) {
	ctx := context.Background()
	p := NewProvider("mock", WithScript(
		Step{Delay: time.Minute, Took: true}, // times out after taking it
		Step{Err: errors.New("boom")},        // fails without taking it
	))
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	taken := domain.Notification{IdempotencyKey: "k1", Channel: domain.ChannelSMS}
	if _, err := p.Send(short, taken); !errors.Is(err, ports.ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	failed := domain.Notification{IdempotencyKey: "k2", Channel: domain.ChannelSMS}
	_, _ = p.Send(ctx, failed)

	if id, found, err := p.Status(ctx, "k1", domain.ChannelSMS); err != nil || !found || id == "" {
		t.Errorf("taken message: %q %v %v", id, found, err)
	}
	if _, found, err := p.Status(ctx, "k2", domain.ChannelSMS); err != nil || found {
		t.Errorf("failed message: found %v err %v", found, err)
	}
	if _, found, _ := p.Status(ctx, "k1", domain.ChannelEmail); found {
		t.Error("status matched a different channel")
	}
}

func TestStatusError(t *testing.T) {
	boom := errors.New("boom")
	p := NewProvider("mock", WithStatusError(boom))
	if _, _, err := p.Status(context.Background(), "k", domain.ChannelSMS); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestWithoutStatusCheckHidesStatus(t *testing.T) {
	p := WithoutStatusCheck(NewProvider("mock"))
	if _, ok := p.(ports.StatusChecker); ok {
		t.Error("wrapped provider still implements StatusChecker")
	}
	if p.Name() != "mock" {
		t.Errorf("name = %q", p.Name())
	}
}

func TestLateFailureIsReportedAfterAccepting(t *testing.T) {
	events := make(chan Event, 1)
	p := NewProvider("mock", WithLateFailure(10*time.Millisecond, func(e Event) { events <- e }))
	id, err := p.Send(context.Background(), domain.Notification{IdempotencyKey: "k1", Channel: domain.ChannelEmail})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.MessageID != id || e.IdempotencyKey != "k1" || e.Channel != domain.ChannelEmail || e.Status != domain.StateFailed || e.Provider != "mock" || e.Reason == "" || e.EventID == "" {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
}

func TestLateDeliveryIsReportedAfterAccepting(t *testing.T) {
	events := make(chan Event, 1)
	p := NewProvider("mock", WithLateDelivery(0, func(e Event) { events <- e }))
	id, err := p.Send(context.Background(), domain.Notification{IdempotencyKey: "k1", Channel: domain.ChannelSMS})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.MessageID != id || e.Status != domain.StateDelivered || e.Reason != "" || e.Channel != domain.ChannelSMS {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
}

func TestEventIDsAreUniquePerEvent(t *testing.T) {
	events := make(chan Event, 3)
	p := NewProvider("mock", WithLateDelivery(0, func(e Event) { events <- e }))
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := p.Send(context.Background(), domain.Notification{IdempotencyKey: k, Channel: domain.ChannelSMS}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case e := <-events:
			if seen[e.EventID] {
				t.Errorf("event id %q repeated", e.EventID)
			}
			seen[e.EventID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("missing event")
		}
	}
}

func TestIDsDoNotRepeatAfterARestart(t *testing.T) {
	ids, events := map[string]bool{}, make(chan Event, 2)
	for i := 0; i < 2; i++ { // two Providers of the same name, as before and after a restart
		p := NewProvider("sms-a", WithLateDelivery(0, func(e Event) { events <- e }))
		id, err := p.Send(context.Background(), domain.Notification{IdempotencyKey: "k", Channel: domain.ChannelSMS})
		if err != nil {
			t.Fatal(err)
		}
		if ids[id] {
			t.Errorf("message id %q repeated", id)
		}
		ids[id] = true
		select {
		case e := <-events:
			if ids[e.EventID] {
				t.Errorf("event id %q repeated", e.EventID)
			}
			ids[e.EventID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
		}
	}
}

func TestEventJSONCarriesEveryFieldTheEndpointReads(t *testing.T) {
	e := Event{
		Provider: "sms-a", EventID: "sms-a-evt-1", MessageID: "sms-a-1", IdempotencyKey: "k1",
		Channel: domain.ChannelSMS, Status: domain.StateFailed, Reason: "الرقم غير صالح <x>",
	}
	var got map[string]string
	if err := json.Unmarshal(e.JSON(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"event_id": "sms-a-evt-1", "message_id": "sms-a-1", "idempotency_key": "k1",
		"channel": "sms", "status": "failed", "reason": "الرقم غير صالح <x>",
	}
	if len(got) != len(want) {
		t.Errorf("fields = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if !strings.Contains(string(e.JSON()), "الرقم غير صالح") {
		t.Errorf("Arabic was escaped: %s", e.JSON())
	}
}

func TestExpiredContextIsNotServed(t *testing.T) {
	p := NewProvider("mock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Send(ctx, domain.Notification{}); !errors.Is(err, context.Canceled) {
		t.Errorf("send err = %v, want canceled", err)
	}
	if len(p.Sent()) != 0 {
		t.Error("a send with a cancelled context was taken")
	}
	if _, _, err := p.Status(ctx, "k", domain.ChannelSMS); !errors.Is(err, context.Canceled) {
		t.Errorf("status err = %v, want canceled", err)
	}
}
