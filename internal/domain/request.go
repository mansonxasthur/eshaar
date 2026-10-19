package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// MaxIdempotencyKeyLen bounds the key, which is used as the Kafka message key
// and as an indexed database column.
const MaxIdempotencyKeyLen = 255

// Request is one notification for one user, as the dispatcher accepts it. The
// sender splits it into one Notification per channel in Content.
type Request struct {
	IdempotencyKey   string                      `json:"idempotency_key"`
	RecipientID      string                      `json:"recipient_id"`
	NotificationType string                      `json:"notification_type"`
	Email            string                      `json:"email,omitempty"`
	Phone            string                      `json:"phone,omitempty"`
	Content          map[Channel]json.RawMessage `json:"content"`
}

// FieldError is one validation failure, naming the offending field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError lists every rule a Request broke, so a caller can fix them
// in one round trip.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid request: " + strings.Join(parts, "; ")
}

// Validate checks the request against the contract in the decision log and
// returns a *ValidationError listing every violation, or nil.
func (r Request) Validate() error {
	var fields []FieldError
	add := func(field, msg string) { fields = append(fields, FieldError{field, msg}) }

	switch {
	case strings.TrimSpace(r.IdempotencyKey) == "":
		add("idempotency_key", "is required")
	case r.IdempotencyKey != strings.TrimSpace(r.IdempotencyKey):
		add("idempotency_key", "must not have leading or trailing whitespace")
	case len(r.IdempotencyKey) > MaxIdempotencyKeyLen:
		add("idempotency_key", fmt.Sprintf("must be at most %d bytes", MaxIdempotencyKeyLen))
	}
	if strings.TrimSpace(r.RecipientID) == "" {
		add("recipient_id", "is required")
	}
	if strings.TrimSpace(r.NotificationType) == "" {
		add("notification_type", "is required")
	}
	if len(r.Content) == 0 {
		add("content", "needs at least one channel")
	}

	// Sorted so the error list is stable.
	names := make([]string, 0, len(r.Content))
	for c := range r.Content {
		names = append(names, string(c))
	}
	sort.Strings(names)

	var needEmail, needPhone bool
	for _, name := range names {
		field := "content." + name
		ch, err := ParseChannel(name)
		if err != nil {
			add(field, "unknown channel")
			continue
		}
		switch ch {
		case ChannelEmail:
			needEmail = true
		case ChannelSMS, ChannelWhatsApp:
			needPhone = true
		}
		if err := validateBlock(ch, r.Content[ch]); err != nil {
			add(field, err.Error())
		}
	}
	if needEmail && strings.TrimSpace(r.Email) == "" {
		add("email", "is required when content has an email channel")
	}
	if needPhone && strings.TrimSpace(r.Phone) == "" {
		add("phone", "is required when content has an sms or whatsapp channel")
	}

	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// validateBlock checks one channel's content: SMS carries free text, email and
// WhatsApp carry a template ID plus optional variables. Unknown keys are
// rejected so a typo is caught here and not at the provider.
func validateBlock(ch Channel, raw json.RawMessage) error {
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if ch == ChannelSMS {
		var b struct {
			Text *string `json:"text"`
		}
		if err := dec.Decode(&b); err != nil {
			return fmt.Errorf("invalid block: %w", err)
		}
		if b.Text == nil || strings.TrimSpace(*b.Text) == "" {
			return errors.New("text is required")
		}
		return nil
	}
	var b struct {
		TemplateID *string                    `json:"template_id"`
		Variables  map[string]json.RawMessage `json:"variables"` // null or absent is allowed
	}
	if err := dec.Decode(&b); err != nil {
		return fmt.Errorf("invalid block: %w", err)
	}
	if b.TemplateID == nil || strings.TrimSpace(*b.TemplateID) == "" {
		return errors.New("template_id is required")
	}
	return nil
}
