package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

type fakePublisher struct {
	err  error
	got  []domain.Request
	hits int
}

func (f *fakePublisher) Publish(_ context.Context, req domain.Request) error {
	f.hits++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, req)
	return nil
}

const validBody = `{
  "idempotency_key": "match-1",
  "recipient_id": "u1",
  "notification_type": "match_started",
  "phone": "+966500000000",
  "content": {"sms": {"text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}}
}`

func newTestServer(t *testing.T, pub *fakePublisher) http.Handler {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	rl := newLimiter(t, []string{"k"}, 1000, 1000)
	return NewHandler(pub, log).Routes(APIKeyAuth([]string{"k"}), rl.Middleware)
}

func post(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/notifications", strings.NewReader(body))
	if key != "" {
		req.Header.Set(APIKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateNotificationAccepted(t *testing.T) {
	pub := &fakePublisher{}
	rec := post(newTestServer(t, pub), "k", validBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if len(pub.got) != 1 || pub.got[0].IdempotencyKey != "match-1" {
		t.Fatalf("published %+v", pub.got)
	}
	if got := string(pub.got[0].Content[domain.ChannelSMS]); !strings.Contains(got, "الهلال والنصر") {
		t.Errorf("Arabic text lost: %s", got)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp["status"] != "accepted" {
		t.Errorf("response = %s", rec.Body)
	}
}

func TestCreateNotificationRejections(t *testing.T) {
	tests := []struct {
		name string
		key  string
		body string
		want int
	}{
		{"no key", "", validBody, http.StatusUnauthorized},
		{"wrong key", "nope", validBody, http.StatusUnauthorized},
		{"empty body", "k", ``, http.StatusBadRequest},
		{"malformed JSON", "k", `{`, http.StatusBadRequest},
		{"unknown top-level field", "k", `{"idempotency_key":"a","bogus":1}`, http.StatusBadRequest},
		{"trailing data", "k", validBody + `{}`, http.StatusBadRequest},
		{"missing phone for sms", "k", strings.Replace(validBody, `"phone": "+966500000000",`, "", 1), http.StatusBadRequest},
		{"too large", "k", `{"idempotency_key":"` + strings.Repeat("a", maxBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub := &fakePublisher{}
			rec := post(newTestServer(t, pub), tt.key, tt.body)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d (%s)", rec.Code, tt.want, rec.Body)
			}
			if pub.hits != 0 {
				t.Error("a rejected request reached the publisher")
			}
		})
	}
}

func TestValidationErrorListsFields(t *testing.T) {
	rec := post(newTestServer(t, &fakePublisher{}), "k", `{"content":{"sms":{"text":"x"}}}`)
	var resp struct {
		Fields []domain.FieldError `json:"fields"`
	}
	if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	got := map[string]bool{}
	for _, f := range resp.Fields {
		got[f.Field] = true
	}
	for _, want := range []string{"idempotency_key", "recipient_id", "notification_type", "phone"} {
		if !got[want] {
			t.Errorf("no error for %s in %s", want, rec.Body)
		}
	}
}

func TestPublishFailureIsNever202(t *testing.T) {
	rec := post(newTestServer(t, &fakePublisher{err: errors.New("broker down: secret-host:9092")}), "k", validBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret-host") {
		t.Errorf("internal detail leaked to the caller: %s", rec.Body)
	}
}

func TestHealthzNeedsNoKey(t *testing.T) {
	h := newTestServer(t, &fakePublisher{})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
