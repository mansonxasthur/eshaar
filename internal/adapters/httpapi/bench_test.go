package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// nopPublisher confirms every message at once and keeps nothing, so a
// benchmark measures the handler and not a growing slice. It is safe to share.
type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, domain.Request) error { return nil }

func benchHandler(b *testing.B) http.Handler {
	b.Helper()
	// Limits so high that the benchmark never sees a 429.
	rl, err := NewRateLimiter([]string{"k"}, 1e9, 1<<30)
	if err != nil {
		b.Fatal(err)
	}
	return NewHandler(nopPublisher{}, slog.New(slog.DiscardHandler)).Routes(APIKeyAuth([]string{"k"}), rl.Middleware)
}

// serve sends one request and returns the status code. It never fails the
// benchmark itself, so parallel workers can report through b.Error.
func serve(h http.Handler) int {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/notifications", strings.NewReader(validBody))
	req.Header.Set(APIKeyHeader, "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// BenchmarkCreateNotification is the dispatcher's work per request without the
// network and Kafka: key check, rate limit, decode, validate, publish, answer.
func BenchmarkCreateNotification(b *testing.B) {
	h := benchHandler(b)
	b.ReportAllocs()
	for b.Loop() {
		if code := serve(h); code != http.StatusAccepted {
			b.Fatalf("status = %d", code)
		}
	}
}

// BenchmarkCreateNotificationParallel runs the same on every CPU, which shows
// contention in the shared middleware (the rate limit bucket is the one lock).
func BenchmarkCreateNotificationParallel(b *testing.B) {
	h := benchHandler(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if code := serve(h); code != http.StatusAccepted {
				b.Errorf("status = %d", code)
				return
			}
		}
	})
}

// BenchmarkAPIKeyAuth is the key check alone. B/op includes the recorder the
// harness allocates for each call.
func BenchmarkAPIKeyAuth(b *testing.B) {
	h := APIKeyAuth([]string{"k1", "k2", "k3", "k4"})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", http.NoBody)
	req.Header.Set(APIKeyHeader, "k3")
	b.ReportAllocs()
	for b.Loop() {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}

// BenchmarkWebhookReceiveHandler is the webhook endpoint's HTTP work with a
// receiver that accepts at once. The request and recorder are built in the loop,
// so ns/op and B/op include the httptest harness, which is most of the B/op.
func BenchmarkWebhookReceiveHandler(b *testing.B) {
	h := NewWebhookHandler(nopReceiver{}, slog.New(slog.DiscardHandler)).Routes()
	body := `{"event_id":"e1","message_id":"m1","channel":"sms","status":"delivered"}`
	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/webhooks/sms-a", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d", rec.Code)
		}
		_, _ = io.Copy(io.Discard, rec.Body)
	}
}

type nopReceiver struct{}

func (nopReceiver) Receive(context.Context, string, []byte, string) error { return nil }
