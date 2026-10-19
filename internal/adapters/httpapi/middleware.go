// Package httpapi implements the dispatcher's HTTP surface.
package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// APIKeyHeader carries the caller's API key.
const APIKeyHeader = "X-API-Key"

// Middleware wraps a handler.
type Middleware func(http.Handler) http.Handler

type ctxKey struct{}

// callerID identifies the authenticated key without exposing it.
type callerID [sha256.Size]byte

// APIKeyAuth rejects requests whose X-API-Key is not one of keys, with 401. It
// validates the key and nothing else: there are no per-key permissions. The
// matched key's identity is put on the request context for the rate limiter.
// Empty keys are ignored, so a blank configured key never authenticates.
func APIKeyAuth(keys []string) Middleware {
	hashed := make([]callerID, 0, len(keys))
	for _, k := range keys {
		if k != "" {
			hashed = append(hashed, sha256.Sum256([]byte(k)))
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(APIKeyHeader)
			if key == "" {
				writeError(w, http.StatusUnauthorized, "invalid or missing API key")
				return
			}
			got := sha256.Sum256([]byte(key))
			// Compare against every key, without an early exit, so timing does
			// not reveal which key matched.
			found := 0
			for _, h := range hashed {
				found |= subtle.ConstantTimeCompare(got[:], h[:])
			}
			if found == 0 {
				writeError(w, http.StatusUnauthorized, "invalid or missing API key")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, callerID(got))))
		})
	}
}

// RateLimiter is an in-memory token bucket per API key. With several
// dispatcher instances each one limits on its own, so the effective limit is
// per instance times the instance count (accepted in the decision log).
type RateLimiter struct {
	buckets    map[callerID]*rate.Limiter // built once, so lookups need no lock
	retryAfter string
	now        func() time.Time
}

// NewRateLimiter gives each of keys its own bucket: rps requests per second,
// with bursts up to burst.
func NewRateLimiter(keys []string, rps float64, burst int) (*RateLimiter, error) {
	if rps <= 0 || burst < 1 {
		return nil, fmt.Errorf("rate limit: rps must be positive and burst at least 1 (got %v, %d)", rps, burst)
	}
	l := &RateLimiter{
		buckets:    make(map[callerID]*rate.Limiter, len(keys)),
		retryAfter: strconv.Itoa(int(math.Ceil(1 / rps))), // time for one token to refill
		now:        time.Now,
	}
	for _, k := range keys {
		if k != "" {
			l.buckets[sha256.Sum256([]byte(k))] = rate.NewLimiter(rate.Limit(rps), burst)
		}
	}
	return l, nil
}

// Middleware answers 429 with Retry-After once a key's bucket is empty. It must
// run after APIKeyAuth; a request that reaches it without an authenticated key
// is a wiring mistake and gets 500.
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := r.Context().Value(ctxKey{}).(callerID)
		lim, ok := l.buckets[id]
		if !ok {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		// AllowN takes a token only when one is available, so rejected
		// requests never drain the bucket, even under concurrency.
		if !lim.AllowN(l.now(), 1) {
			w.Header().Set("Retry-After", l.retryAfter)
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Chain applies middlewares so that the first one listed runs first.
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
