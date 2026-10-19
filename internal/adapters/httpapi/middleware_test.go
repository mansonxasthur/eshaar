package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var ok200 = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func do(h http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
	if key != "" {
		req.Header.Set(APIKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAPIKeyAuth(t *testing.T) {
	h := APIKeyAuth([]string{"alpha", "beta"})(ok200)
	tests := []struct {
		name string
		key  string
		want int
	}{
		{"missing", "", http.StatusUnauthorized},
		{"wrong", "gamma", http.StatusUnauthorized},
		{"prefix of a key", "alph", http.StatusUnauthorized},
		{"first key", "alpha", http.StatusOK},
		{"second key", "beta", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := do(h, tt.key).Code; got != tt.want {
				t.Errorf("status = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAPIKeyAuthWithNoKeysRejectsEverything(t *testing.T) {
	h := APIKeyAuth(nil)(ok200)
	if got := do(h, "anything").Code; got != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", got)
	}
}

func TestRateLimiter(t *testing.T) {
	now := time.Unix(1000, 0)
	rl := newLimiter(t, []string{"a", "b"}, 1, 2) // 1 req/s, burst 2
	rl.now = func() time.Time { return now }
	h := Chain(ok200, APIKeyAuth([]string{"a", "b"}), rl.Middleware)

	for i := range 2 {
		if got := do(h, "a").Code; got != http.StatusOK {
			t.Fatalf("burst request %d: status %d, want 200", i, got)
		}
	}
	rec := do(h, "a")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 has no Retry-After")
	}
	if got := do(h, "b").Code; got != http.StatusOK {
		t.Errorf("another key shares the bucket: status %d, want 200", got)
	}

	now = now.Add(time.Second) // one token refills
	if got := do(h, "a").Code; got != http.StatusOK {
		t.Errorf("after refill: status %d, want 200", got)
	}
	if got := do(h, "a").Code; got != http.StatusTooManyRequests {
		t.Errorf("after refill: second request status %d, want 429", got)
	}
}

func TestRateLimiterRejectedRequestsDoNotConsumeTokens(t *testing.T) {
	now := time.Unix(1000, 0)
	rl := newLimiter(t, []string{"a"}, 1, 1)
	rl.now = func() time.Time { return now }
	h := Chain(ok200, APIKeyAuth([]string{"a"}), rl.Middleware)

	do(h, "a")
	for range 5 {
		do(h, "a") // all rejected
	}
	now = now.Add(time.Second)
	if got := do(h, "a").Code; got != http.StatusOK {
		t.Errorf("rejected requests drained the bucket: status %d, want 200", got)
	}
}

func TestRateLimiterWithoutAuthIsAnError(t *testing.T) {
	rl := newLimiter(t, []string{"a"}, 1, 1)
	if got := do(rl.Middleware(ok200), "a").Code; got != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (misconfigured chain)", got)
	}
}

func newLimiter(t *testing.T, keys []string, rps float64, burst int) *RateLimiter {
	t.Helper()
	rl, err := NewRateLimiter(keys, rps, burst)
	if err != nil {
		t.Fatal(err)
	}
	return rl
}

func TestNewRateLimiterRejectsBadSettings(t *testing.T) {
	for _, c := range []struct {
		rps   float64
		burst int
	}{{0, 1}, {-1, 1}, {1, 0}} {
		if _, err := NewRateLimiter([]string{"a"}, c.rps, c.burst); err == nil {
			t.Errorf("rps=%v burst=%d accepted", c.rps, c.burst)
		}
	}
}

func TestAPIKeyAuthIgnoresEmptyConfiguredKey(t *testing.T) {
	h := APIKeyAuth([]string{"", "a"})(ok200)
	if got := do(h, ""); got.Code != http.StatusUnauthorized {
		t.Errorf("empty header status = %d, want 401", got.Code)
	}
}

func TestRateLimiterConcurrentRejectionsKeepTheBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	rl := newLimiter(t, []string{"a"}, 1, 1)
	rl.now = func() time.Time { return now }
	h := Chain(ok200, APIKeyAuth([]string{"a"}), rl.Middleware)
	do(h, "a") // empties the bucket

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { do(h, "a") })
	}
	wg.Wait()

	now = now.Add(time.Second)
	if got := do(h, "a").Code; got != http.StatusOK {
		t.Errorf("concurrent rejections left the bucket in debt: status %d, want 200", got)
	}
}
