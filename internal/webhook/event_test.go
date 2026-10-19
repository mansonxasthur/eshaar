package webhook

import (
	"errors"
	"strings"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

const goodBody = `{"event_id":"e1","message_id":"m1","idempotency_key":"k1","channel":"sms","status":"failed","reason":"carrier rejected"}`

func TestParseEventReadsAFailure(t *testing.T) {
	ev, err := ParseEvent("sms-a", []byte(goodBody))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Provider != "sms-a" || ev.EventID != "e1" || ev.ProviderMessageID != "m1" || ev.IdempotencyKey != "k1" ||
		ev.Channel != domain.ChannelSMS || ev.Status != domain.StateFailed || ev.Reason != "carrier rejected" {
		t.Errorf("event = %+v", ev)
	}
	if string(ev.Payload) != goodBody {
		t.Errorf("payload = %s, want the body as received", ev.Payload)
	}
}

func TestParseEventReadsDeliveredWithOptionalFieldsMissing(t *testing.T) {
	ev, err := ParseEvent("email-a", []byte(`{"message_id":"m1","channel":"email","status":"delivered"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != domain.StateDelivered || ev.EventID != "" || ev.IdempotencyKey != "" || ev.Reason != "" {
		t.Errorf("event = %+v", ev)
	}
}

func TestParseEventKeepsTheBodyWhole(t *testing.T) {
	body := `{"message_id":"m1","channel":"sms","status":"delivered","extra":{"carrier":"x"}}`
	ev, err := ParseEvent("sms-a", []byte(body))
	if err != nil {
		t.Fatalf("unknown field rejected: %v", err)
	}
	if string(ev.Payload) != body {
		t.Errorf("payload = %s", ev.Payload)
	}
}

func TestParseEventDoesNotAliasTheBody(t *testing.T) {
	body := []byte(goodBody)
	ev, err := ParseEvent("sms-a", body)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	if string(ev.Payload) != goodBody {
		t.Error("payload shares the caller's buffer")
	}
}

func TestParseEventRejectsBadBodies(t *testing.T) {
	cases := map[string]string{
		"empty":               ``,
		"not json":            `delivered`,
		"array":               `[]`,
		"null":                `null`,
		"number field":        `{"message_id":1,"channel":"sms","status":"delivered"}`,
		"trailing data":       goodBody + `{}`,
		"trailing garbage":    goodBody + ` x`,
		"no message id":       `{"channel":"sms","status":"delivered"}`,
		"empty message id":    `{"message_id":"","channel":"sms","status":"delivered"}`,
		"no channel":          `{"message_id":"m","status":"delivered"}`,
		"unknown channel":     `{"message_id":"m","channel":"fax","status":"delivered"}`,
		"no status":           `{"message_id":"m","channel":"sms"}`,
		"accepted status":     `{"message_id":"m","channel":"sms","status":"accepted"}`,
		"pending status":      `{"message_id":"m","channel":"sms","status":"pending"}`,
		"unresolved status":   `{"message_id":"m","channel":"sms","status":"unresolved"}`,
		"capitalized status":  `{"message_id":"m","channel":"sms","status":"Delivered"}`,
		"nul in a field":      `{"message_id":"m\u0000","channel":"sms","status":"delivered"}`,
		"nul in unknown":      `{"message_id":"m","channel":"sms","status":"delivered","x":"\u0000"}`,
		"lone high surrogate": `{"message_id":"m","channel":"sms","status":"failed","reason":"\ud83d"}`,
		"high then text":      `{"message_id":"m","channel":"sms","status":"failed","reason":"\ud83dabcdefgh"}`,
		"high then other esc": `{"message_id":"m","channel":"sms","status":"failed","reason":"\ud83d\u0041"}`,
		"lone low surrogate":  `{"message_id":"m","channel":"sms","status":"failed","reason":"\ude00"}`,
		"nul after pair":      `{"message_id":"m","channel":"sms","status":"failed","reason":"\ud83d\ude00\u0000"}`,
		"invalid utf-8":       "{\"message_id\":\"m\xff\",\"channel\":\"sms\",\"status\":\"delivered\"}",
		"long message id":     `{"message_id":"` + strings.Repeat("a", 256) + `","channel":"sms","status":"delivered"}`,
		"long event id":       `{"event_id":"` + strings.Repeat("a", 256) + `","message_id":"m","channel":"sms","status":"delivered"}`,
		"long idempotent key": `{"idempotency_key":"` + strings.Repeat("a", 256) + `","message_id":"m","channel":"sms","status":"delivered"}`,
	}
	for name, body := range cases {
		if _, err := ParseEvent("sms-a", []byte(body)); !errors.Is(err, ports.ErrInvalidEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidEvent", name, err)
		}
	}
}

func TestParseEventReportsEveryProblem(t *testing.T) {
	_, err := ParseEvent("sms-a", []byte(`{"channel":"fax","status":"sent"}`))
	if err == nil {
		t.Fatal("accepted")
	}
	for _, want := range []string{"message_id is required", "channel must be", "status must be"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

func TestParseEventAcceptsIDsAtTheLimit(t *testing.T) {
	id := strings.Repeat("a", 255)
	if _, err := ParseEvent("sms-a", []byte(`{"event_id":"`+id+`","message_id":"`+id+`","idempotency_key":"`+id+`","channel":"sms","status":"delivered"}`)); err != nil {
		t.Errorf("ids of 255 bytes rejected: %v", err)
	}
}

func TestParseEventKeepsArabicAndCutsTheReasonByRunes(t *testing.T) {
	reason := strings.Repeat("خطأ", 400) // 1200 runes, 2400 bytes
	ev, err := ParseEvent("sms-a", []byte(`{"message_id":"م1","channel":"sms","status":"failed","reason":"`+reason+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.ProviderMessageID != "م1" {
		t.Errorf("message id = %q", ev.ProviderMessageID)
	}
	if got := []rune(ev.Reason); len(got) != 500 || ev.Reason != string([]rune(reason)[:500]) {
		t.Errorf("reason has %d runes, want the first 500 intact", len(got))
	}
	short, err := ParseEvent("sms-a", []byte(`{"message_id":"m","channel":"sms","status":"failed","reason":"الرقم غير صالح"}`))
	if err != nil || short.Reason != "الرقم غير صالح" {
		t.Errorf("short Arabic reason = %q, %v", short.Reason, err)
	}
}

func TestParseEventAcceptsEscapesPostgresCanStore(t *testing.T) {
	for name, reason := range map[string]string{
		"surrogate pair":           `\ud83d\ude00`,
		"upper-case pair":          `\uD83D\uDE00`,
		"escaped backslash u0000":  `\\u0000`,
		"backslash then pair":      `\\\ud83d\ude00`,
		"ordinary escapes":         `line\nbreak \"quoted\" \u0627`,
		"escaped backslash at end": `path\\`,
	} {
		body := `{"message_id":"m","channel":"sms","status":"failed","reason":"` + reason + `"}`
		if _, err := ParseEvent("sms-a", []byte(body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
