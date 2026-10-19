package webhook

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// BenchmarkSign is the HMAC a provider computes, and the cost of Verify's work.
func BenchmarkSign(b *testing.B) {
	body := signedBody("e1")
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		_ = Sign(secretA, body)
	}
}

// BenchmarkVerify is the signature check done for every webhook.
func BenchmarkVerify(b *testing.B) {
	v, err := NewVerifier(map[string]string{"sms-a": secretA})
	if err != nil {
		b.Fatal(err)
	}
	body := signedBody("e1")
	sig := Sign(secretA, body)
	b.ReportAllocs()
	for b.Loop() {
		if err := v.Verify("sms-a", body, sig); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseEvent is the parsing and storability check of one event body.
func BenchmarkParseEvent(b *testing.B) {
	body := signedBody("e1")
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ParseEvent("sms-a", body); err != nil {
			b.Fatal(err)
		}
	}
}

// discardStore takes events and keeps nothing.
type discardStore struct{}

func (discardStore) RecordEvents(context.Context, []domain.WebhookEvent) error { return nil }
func (discardStore) ResolvePending(context.Context, int) (int, error)          { return 0, nil }

// BenchmarkReceiveParallel is the whole service path (verify, parse, queue,
// batch, wait for the flush) against a store that answers at once, with the
// production batch settings and about 1,000 requests waiting, as an HTTP server
// has. Each request waits for its batch, so ns/op is the service's throughput
// cost, not one request's latency. It shows the batching without Postgres.
func BenchmarkReceiveParallel(b *testing.B) {
	cfg := Config{
		BatchSize: 200, FlushInterval: 20 * time.Millisecond, Flushers: 4, FlushTimeout: 5 * time.Second,
		QueueSize: 1600, ResolveInterval: time.Second, ResolveBatch: 200,
	}
	// About 1,000 waiting requests on any CPU count, under the queue size.
	b.SetParallelism(max(1, 1000/runtime.GOMAXPROCS(0)))
	s, err := New(discardStore{}, testProviders, cfg, discard())
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Run(ctx) }()
	b.Cleanup(func() { cancel(); <-done })

	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := strconv.FormatInt(seq.Add(1), 10)
			if err := receive(s, "sms-a", secretA, signedBody(id)); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
