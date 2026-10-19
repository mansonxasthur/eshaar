package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/ports"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

type fakeReceiver struct {
	err       error
	provider  string
	body      string
	signature string
	calls     int
}

func (f *fakeReceiver) Receive(_ context.Context, provider string, body []byte, signature string) error {
	f.calls++
	f.provider, f.body, f.signature = provider, string(body), signature
	return f.err
}

func postWebhook(t *testing.T, r ports.EventReceiver, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	NewWebhookHandler(r, discardLogger()).Routes().ServeHTTP(rec, req)
	return rec
}

func TestWebhookPassesTheRawBodySignatureAndProvider(t *testing.T) {
	f := &fakeReceiver{}
	body := `{"message_id":"م1"}`
	rec := postWebhook(t, f, "/v1/webhooks/sms-a", body, map[string]string{SignatureHeader: "abc123"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	if f.provider != "sms-a" || f.body != body || f.signature != "abc123" {
		t.Errorf("receiver got %q %q %q", f.provider, f.body, f.signature)
	}
}

func TestWebhookStatusCodes(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
	}{
		"stored":            {nil, http.StatusOK},
		"bad signature":     {ports.ErrInvalidSignature, http.StatusUnauthorized},
		"unknown provider":  {fmt.Errorf("%w: x", ports.ErrUnknownProvider), http.StatusUnauthorized},
		"invalid event":     {fmt.Errorf("%w: status must be delivered or failed", ports.ErrInvalidEvent), http.StatusBadRequest},
		"store failure":     {errors.New("database down"), http.StatusServiceUnavailable},
		"shutting down":     {errors.New("webhook service is shutting down"), http.StatusServiceUnavailable},
		"request cancelled": {context.Canceled, http.StatusServiceUnavailable},
	}
	for name, c := range cases {
		rec := postWebhook(t, &fakeReceiver{err: c.err}, "/v1/webhooks/p", `{}`, nil)
		if rec.Code != c.want {
			t.Errorf("%s: status = %d, want %d", name, rec.Code, c.want)
		}
	}
}

func TestWebhookAnswersAnUnknownProviderExactlyLikeABadSignature(t *testing.T) {
	bad := postWebhook(t, &fakeReceiver{err: ports.ErrInvalidSignature}, "/v1/webhooks/sms-a", `{}`, nil)
	unknown := postWebhook(t, &fakeReceiver{err: fmt.Errorf("%w: %q", ports.ErrUnknownProvider, "nope")}, "/v1/webhooks/nope", `{}`, nil)
	if bad.Code != unknown.Code || bad.Body.String() != unknown.Body.String() || bad.Header().Get("Content-Type") != unknown.Header().Get("Content-Type") {
		t.Errorf("responses differ: %d %q vs %d %q", bad.Code, bad.Body, unknown.Code, unknown.Body)
	}
}

func TestWebhookDoesNotLeakStoreErrors(t *testing.T) {
	rec := postWebhook(t, &fakeReceiver{err: errors.New("pq: password authentication failed for user eshaar")}, "/v1/webhooks/p", `{}`, nil)
	if strings.Contains(rec.Body.String(), "password") {
		t.Errorf("body leaks the error: %s", rec.Body)
	}
}

func TestWebhookRejectsALargeBodyWithoutCallingTheReceiver(t *testing.T) {
	f := &fakeReceiver{}
	rec := postWebhook(t, f, "/v1/webhooks/p", strings.Repeat("a", maxWebhookBytes+1), nil)
	if rec.Code != http.StatusRequestEntityTooLarge || f.calls != 0 {
		t.Errorf("status = %d, receiver called %d times", rec.Code, f.calls)
	}
	f = &fakeReceiver{}
	if rec = postWebhook(t, f, "/v1/webhooks/p", strings.Repeat("a", maxWebhookBytes), nil); rec.Code != http.StatusOK {
		t.Errorf("a body at the limit: status = %d", rec.Code)
	}
}

func TestWebhookRoutes(t *testing.T) {
	h := NewWebhookHandler(&fakeReceiver{}, discardLogger()).Routes()
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/v1/webhooks/sms-a", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/webhooks/", http.StatusNotFound},
		{http.MethodPost, "/v1/notifications", http.StatusNotFound},
		{http.MethodGet, "/healthz", http.StatusOK},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), c.method, c.path, http.NoBody))
		if rec.Code != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.want)
		}
	}
}
