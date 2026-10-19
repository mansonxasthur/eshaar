package domain

import (
	"encoding/json"
	"testing"
	"time"
)

// fullRequest is a request with all three channels, the worst case the
// dispatcher validates and the sender splits.
func fullRequest() Request {
	return Request{
		IdempotencyKey:   "match-1",
		RecipientID:      "u1",
		NotificationType: "match_started",
		Email:            "a@example.com",
		Phone:            "+966500000000",
		Content: map[Channel]json.RawMessage{
			ChannelEmail:    json.RawMessage(`{"template_id":"match_started_v2","variables":{"team_a":"الهلال","team_b":"النصر"}}`),
			ChannelSMS:      json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
			ChannelWhatsApp: json.RawMessage(`{"template_id":"match_started","variables":{"team_a":"الهلال","team_b":"النصر"}}`),
		},
	}
}

// BenchmarkRequestValidate is the per-request check at the dispatcher.
func BenchmarkRequestValidate(b *testing.B) {
	req := fullRequest()
	b.ReportAllocs()
	for b.Loop() {
		if err := req.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRequestNotifications is the per-message split into rows at the sender.
func BenchmarkRequestNotifications(b *testing.B) {
	req := fullRequest()
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		if got := req.Notifications(now); len(got) != 3 {
			b.Fatalf("got %d rows", len(got))
		}
	}
}
