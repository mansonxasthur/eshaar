package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
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

// payload holds the fields ParseEvent reads from the body.
type payload struct {
	EventID        string
	MessageID      string
	IdempotencyKey string
	Channel        string
	Status         string
	Reason         string
}

// field returns where the JSON key goes, or nil for a key we do not read. Keys
// are matched exactly, not case-insensitively as a struct decode would, because
// the Postgres store reads the same keys from the stored payload with a
// case-sensitive operator: the two must agree on which events carry a status
// and a key. Unknown fields are ignored (the raw body is kept whole), so a
// provider can add fields. A JSON null reads as an absent field.
func (p *payload) field(key string) *string {
	switch key {
	case "event_id":
		return &p.EventID
	case "message_id":
		return &p.MessageID
	case "idempotency_key":
		return &p.IdempotencyKey
	case "channel":
		return &p.Channel
	case "status":
		return &p.Status
	case "reason":
		return &p.Reason
	}
	return nil
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
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&raw); err != nil || raw == nil {
		return domain.WebhookEvent{}, invalid("body is not a JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return domain.WebhookEvent{}, invalid("body has data after the JSON object")
	}
	var p payload
	for name, v := range raw {
		if dst := p.field(name); dst != nil {
			if err := json.Unmarshal(v, dst); err != nil {
				return domain.WebhookEvent{}, invalid(name + " must be a string")
			}
		}
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

// maxNumberLength and maxExponent bound the numbers of a body. Postgres jsonb
// stores arbitrary decimals but fails on an exponent beyond its numeric range
// (1e999999999), which Go's decoder leaves as text. Events carry no numbers we
// read, so the bounds are generous.
const (
	maxNumberLength = 64
	maxExponent     = 300
)

// checkStorable rejects what Postgres refuses in jsonb but Go's decoder accepts:
// the \u0000 escape, a surrogate escape that is not half of a high and low
// pair, and a number with an exponent out of range. It scans the raw text once.
// body is already valid JSON, so every backslash is inside a string, an escaped
// backslash followed by u0000 is skipped as the two-byte escape it is, and a
// digit outside a string starts a number.
func checkStorable(body []byte) error {
	inString := false
	for i := 0; i < len(body); i++ {
		c := body[i]
		if !inString {
			switch {
			case c == '"':
				inString = true
			case c == '-' || (c >= '0' && c <= '9'):
				end := i + 1
				for end < len(body) && strings.IndexByte("0123456789+-.eE", body[end]) >= 0 {
					end++
				}
				if err := checkNumber(body[i:end]); err != nil {
					return err
				}
				i = end - 1
			}
			continue
		}
		switch c {
		case '"':
			inString = false
		case '\\':
			if i+1 >= len(body) {
				return invalid("body ends inside an escape")
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
	}
	return nil
}

// checkNumber rejects a number that is too long or has an exponent beyond
// maxExponent.
func checkNumber(num []byte) error {
	if len(num) > maxNumberLength {
		return invalid("body has a number longer than 64 characters")
	}
	if e := bytes.IndexAny(num, "eE"); e >= 0 {
		exp, err := strconv.Atoi(strings.TrimPrefix(string(num[e+1:]), "+"))
		if err != nil || exp > maxExponent || exp < -maxExponent {
			return invalid("body has a number with an exponent out of range")
		}
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
