package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

// maxIDLength bounds the identifiers an event carries, the same limit the
// dispatcher puts on an idempotency key.
const maxIDLength = 255

// maxReasonRunes caps the reason kept from an event, like the reasons the
// sender writes.
const maxReasonRunes = 500

// payload is the JSON body of an event. Unknown fields are ignored (the raw
// body is kept whole), so a provider can add fields without breaking us.
type payload struct {
	EventID        string `json:"event_id"`
	MessageID      string `json:"message_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Channel        string `json:"channel"`
	Status         string `json:"status"`
	Reason         string `json:"reason"`
}

// ParseEvent reads the body of a webhook from provider. event_id is optional
// (an event without one is never deduplicated by id), idempotency_key is
// optional, and message_id, channel and status are required. status is
// "delivered" or "failed"; anything else is rejected, since the endpoint only
// records final states. Every error wraps ports.ErrInvalidEvent.
//
// The body must be one JSON object in valid UTF-8 that Postgres can store in
// jsonb (see checkStorable): one event it cannot store would fail the whole
// batch it is saved with, and the provider would retry it forever. The Arabic
// text is kept as is.
func ParseEvent(provider string, body []byte) (domain.WebhookEvent, error) {
	if !utf8.Valid(body) {
		return domain.WebhookEvent{}, invalid("body is not valid UTF-8")
	}
	var p payload
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&p); err != nil {
		return domain.WebhookEvent{}, invalid("body is not a JSON object with string fields")
	}
	if _, err := dec.Token(); err != io.EOF {
		return domain.WebhookEvent{}, invalid("body has data after the JSON object")
	}

	if err := checkStorable(body); err != nil {
		return domain.WebhookEvent{}, err
	}

	var problems []string
	for _, f := range []struct{ name, value string }{
		{"event_id", p.EventID}, {"message_id", p.MessageID}, {"idempotency_key", p.IdempotencyKey},
	} {
		if len(f.value) > maxIDLength {
			problems = append(problems, fmt.Sprintf("%s is longer than %d bytes", f.name, maxIDLength))
		}
	}
	if p.MessageID == "" {
		problems = append(problems, "message_id is required")
	}
	channel, err := domain.ParseChannel(p.Channel)
	if err != nil {
		problems = append(problems, "channel must be email, sms or whatsapp")
	}
	status := domain.State(p.Status)
	if status != domain.StateDelivered && status != domain.StateFailed {
		problems = append(problems, "status must be delivered or failed")
	}
	if len(problems) > 0 {
		return domain.WebhookEvent{}, invalid(strings.Join(problems, "; "))
	}

	return domain.WebhookEvent{
		Provider:          provider,
		Channel:           channel,
		EventID:           p.EventID,
		ProviderMessageID: p.MessageID,
		IdempotencyKey:    p.IdempotencyKey,
		Status:            status,
		Reason:            truncate(p.Reason, maxReasonRunes),
		Payload:           json.RawMessage(bytes.Clone(body)),
	}, nil
}

func invalid(msg string) error { return fmt.Errorf("%w: %s", ports.ErrInvalidEvent, msg) }

// truncate keeps at most n runes of s, so Arabic text is never cut inside a
// character.
func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// checkStorable rejects the \u escapes Postgres refuses in jsonb: \u0000, and
// a surrogate that is not half of a high and low pair. Go's decoder accepts
// both, so they are checked on the raw text. body is already valid JSON, so
// every backslash is inside a string, and an escaped backslash followed by
// u0000 is skipped as the two-byte escape it is.
func checkStorable(body []byte) error {
	for i := 0; i < len(body); i++ {
		if body[i] != '\\' {
			continue
		}
		if body[i+1] != 'u' {
			i++ // skip the escaped character, which may itself be a backslash
			continue
		}
		r, ok := hexRune(body[i+2:])
		switch {
		case !ok:
			return invalid("body has a malformed \\u escape")
		case r == 0:
			return invalid("body contains a NUL character")
		case r >= 0xDC00 && r <= 0xDFFF:
			return invalid("body has a low surrogate escape with no high surrogate before it")
		case r >= 0xD800 && r <= 0xDBFF:
			if len(body) < i+12 || body[i+6] != '\\' || body[i+7] != 'u' {
				return invalid("body has a high surrogate escape with no low surrogate after it")
			}
			if low, ok := hexRune(body[i+8:]); !ok || low < 0xDC00 || low > 0xDFFF {
				return invalid("body has a high surrogate escape with no low surrogate after it")
			}
			i += 6 // the low half is skipped with the loop's own step below
		}
		i += 5
	}
	return nil
}

// hexRune reads four hex digits from the start of b.
func hexRune(b []byte) (rune, bool) {
	if len(b) < 4 {
		return 0, false
	}
	var r rune
	for _, c := range b[:4] {
		switch {
		case c >= '0' && c <= '9':
			c -= '0'
		case c >= 'a' && c <= 'f':
			c = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			c = c - 'A' + 10
		default:
			return 0, false
		}
		r = r<<4 | rune(c)
	}
	return r, true
}
