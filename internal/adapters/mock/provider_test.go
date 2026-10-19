package mock

import (
	"context"
	"errors"
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
		if e.MessageID != id || e.IdempotencyKey != "k1" || e.Channel != domain.ChannelEmail || e.Status != domain.StateFailed || e.Provider != "mock" {
			t.Errorf("event = %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
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
