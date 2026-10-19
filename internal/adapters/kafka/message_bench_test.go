package kafka

import (
	"encoding/json"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

func benchRequest() domain.Request {
	return domain.Request{
		IdempotencyKey: "match-1", RecipientID: "u1", NotificationType: "match_started",
		Email: "a@example.com", Phone: "+966500000000",
		Content: map[domain.Channel]json.RawMessage{
			domain.ChannelEmail: json.RawMessage(`{"template_id":"match_started_v2","variables":{"team_a":"الهلال"}}`),
			domain.ChannelSMS:   json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
		},
	}
}

// BenchmarkEncode is the record written by the dispatcher for each request.
func BenchmarkEncode(b *testing.B) {
	req := benchRequest()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := encode(req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecode is the record read by the sender for each message.
func BenchmarkDecode(b *testing.B) {
	value, err := encode(benchRequest())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := decode(value); err != nil {
			b.Fatal(err)
		}
	}
}
