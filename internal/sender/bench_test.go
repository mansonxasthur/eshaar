package sender

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// nopStore takes every write and keeps nothing, so a benchmark measures the
// sender and not a test double that grows and locks.
type nopStore struct{}

func (nopStore) InsertPending(_ context.Context, rows []domain.Notification) ([]domain.Notification, error) {
	return rows, nil
}
func (nopStore) MarkAccepted(context.Context, string, string, string) error { return nil }
func (nopStore) MarkFailed(context.Context, string, string) error           { return nil }
func (nopStore) MarkUnresolved(context.Context, string, string) error       { return nil }
func (nopStore) RecordLastProvider(context.Context, string, string) error   { return nil }

// benchDeliverer builds a one-channel Deliverer over routes and a no-op store.
func benchDeliverer(b *testing.B, routes ...Route) *Deliverer {
	b.Helper()
	d, err := NewDeliverer(nopStore{}, map[domain.Channel][]Route{domain.ChannelSMS: routes},
		DelivererConfig{SendTimeout: time.Second, StatusTimeout: 100 * time.Millisecond}, slog.New(slog.DiscardHandler))
	if err != nil {
		b.Fatal(err)
	}
	return d
}

func benchRoute(p ports.Provider) Route { return Route{Provider: p, Timeout: 500 * time.Millisecond} }

func benchRow() domain.Notification {
	return domain.Notification{ID: "id1", IdempotencyKey: "k1", Channel: domain.ChannelSMS, State: domain.StatePending}
}

// BenchmarkDeliverFirstProvider is the sender's work per channel when the first
// provider accepts: the chain, one attempt, and the outcome write to a no-op store.
func BenchmarkDeliverFirstProvider(b *testing.B) {
	d := benchDeliverer(b, benchRoute(&stub{name: "a", send: accepts("m")}))
	n := benchRow()
	b.ReportAllocs()
	for b.Loop() {
		d.Send(context.Background(), n)
	}
}

// BenchmarkDeliverFallback is the same when the first provider fails and the
// second accepts, so it adds one failed attempt and a log line at warn level.
func BenchmarkDeliverFallback(b *testing.B) {
	fail := func(context.Context, domain.Notification) (string, error) { return "", ports.ErrTransient }
	d := benchDeliverer(b, benchRoute(&stub{name: "a", send: fail}), benchRoute(&stub{name: "b", send: accepts("m")}))
	n := benchRow()
	b.ReportAllocs()
	for b.Loop() {
		d.Send(context.Background(), n)
	}
}

// BenchmarkDeliverParallel runs the first-provider path on every CPU through
// the real breaker and a limiter that never waits, as the wiring builds a provider.
func BenchmarkDeliverParallel(b *testing.B) {
	p := WithBreaker(WithRateLimit(&stub{name: "a", send: accepts("m")}, 1e9), BreakerConfig{FailureThreshold: 5})
	d := benchDeliverer(b, benchRoute(p))
	n := benchRow()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Send(context.Background(), n)
		}
	})
}

// BenchmarkBreakerClosedParallel is the breaker's cost on a healthy provider,
// where every call takes its lock.
func BenchmarkBreakerClosedParallel(b *testing.B) {
	p := WithBreaker(&stub{name: "a", send: accepts("m")}, BreakerConfig{FailureThreshold: 5})
	n := benchRow()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Send(context.Background(), n); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkBreakerOpenParallel is the fast path of an open breaker, the case
// during a provider outage when every call is rejected without being made.
func BenchmarkBreakerOpenParallel(b *testing.B) {
	boom := func(context.Context, domain.Notification) (string, error) { return "", errors.Join(ports.ErrTransient) }
	p := WithBreaker(&stub{name: "a", send: boom}, BreakerConfig{FailureThreshold: 1, OpenFor: time.Hour})
	n := benchRow()
	_, _ = p.Send(context.Background(), n) // opens it
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Send(context.Background(), n); !errors.Is(err, ErrCircuitOpen) {
				b.Errorf("err = %v, want an open breaker", err)
				return
			}
		}
	})
}

// BenchmarkRateLimitParallel is the outbound limiter when it never has to wait.
func BenchmarkRateLimitParallel(b *testing.B) {
	p := WithRateLimit(&stub{name: "a", send: accepts("m")}, 1e9)
	n := benchRow()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Send(context.Background(), n); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
