package mock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
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
