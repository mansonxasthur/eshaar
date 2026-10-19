package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func validRequest() Request {
	return Request{
		IdempotencyKey:   "k1",
		RecipientID:      "u1",
		NotificationType: "match_started",
		Email:            "a@example.com",
		Phone:            "+966500000000",
		Content: map[Channel]json.RawMessage{
			ChannelEmail:    json.RawMessage(`{"template_id":"match_started_v2","variables":{"team_a":"الهلال","team_b":"النصر"}}`),
			ChannelSMS:      json.RawMessage(`{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}`),
			ChannelWhatsApp: json.RawMessage(`{"template_id":"match_started","variables":{"team_a":"الهلال"}}`),
		},
	}
}

func TestRequestValidate(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*Request)
		wantFields []string // empty means valid
	}{
		{"valid with all channels", func(*Request) {}, nil},
		{"sms only without email", func(r *Request) {
			r.Email = ""
			delete(r.Content, ChannelEmail)
			delete(r.Content, ChannelWhatsApp)
		}, nil},
		{"missing idempotency key", func(r *Request) { r.IdempotencyKey = " " }, []string{"idempotency_key"}},
		{"missing recipient", func(r *Request) { r.RecipientID = "" }, []string{"recipient_id"}},
		{"missing type", func(r *Request) { r.NotificationType = "" }, []string{"notification_type"}},
		{"no content", func(r *Request) { r.Content = nil }, []string{"content"}},
		{"email channel without email", func(r *Request) { r.Email = "" }, []string{"email"}},
		{"sms without phone", func(r *Request) {
			r.Phone = ""
			delete(r.Content, ChannelWhatsApp)
		}, []string{"phone"}},
		{"whatsapp without phone", func(r *Request) {
			r.Phone = ""
			delete(r.Content, ChannelSMS)
		}, []string{"phone"}},
		{"unknown channel", func(r *Request) { r.Content["push"] = json.RawMessage(`{}`) }, []string{"content.push"}},
		{"sms without text", func(r *Request) { r.Content[ChannelSMS] = json.RawMessage(`{"text":""}`) }, []string{"content.sms"}},
		{"email without template", func(r *Request) { r.Content[ChannelEmail] = json.RawMessage(`{"variables":{}}`) }, []string{"content.email"}},
		{"block not an object", func(r *Request) { r.Content[ChannelEmail] = json.RawMessage(`"x"`) }, []string{"content.email"}},
		{"variables not an object", func(r *Request) {
			r.Content[ChannelWhatsApp] = json.RawMessage(`{"template_id":"t","variables":[1]}`)
		}, []string{"content.whatsapp"}},
		{"variables null is allowed", func(r *Request) {
			r.Content[ChannelEmail] = json.RawMessage(`{"template_id":"t","variables":null}`)
		}, nil},
		{"whitespace template id", func(r *Request) {
			r.Content[ChannelEmail] = json.RawMessage(`{"template_id":"  "}`)
		}, []string{"content.email"}},
		{"unknown key in sms block", func(r *Request) {
			r.Content[ChannelSMS] = json.RawMessage(`{"text":"hi","templat_id":"x"}`)
		}, []string{"content.sms"}},
		{"text in an email block", func(r *Request) {
			r.Content[ChannelEmail] = json.RawMessage(`{"template_id":"t","text":"x"}`)
		}, []string{"content.email"}},
		{"empty block", func(r *Request) { r.Content[ChannelSMS] = nil }, []string{"content.sms"}},
		{"key with surrounding space", func(r *Request) { r.IdempotencyKey = " k1" }, []string{"idempotency_key"}},
		{"key too long", func(r *Request) { r.IdempotencyKey = strings.Repeat("k", MaxIdempotencyKeyLen+1) }, []string{"idempotency_key"}},
		{"NUL in recipient", func(r *Request) { r.RecipientID = "a\x00b" }, []string{"recipient_id"}},
		{"invalid UTF-8 in key", func(r *Request) { r.IdempotencyKey = "a\xffb" }, []string{"idempotency_key"}},
		{"escaped NUL in content", func(r *Request) {
			r.Content[ChannelSMS] = json.RawMessage(`{"text":"a\u0000b"}`)
		}, []string{"content.sms"}},
		{"several errors at once", func(r *Request) {
			r.IdempotencyKey, r.Email, r.Phone = "", "", ""
		}, []string{"idempotency_key", "email", "phone"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRequest()
			tt.mutate(&r)
			err := r.Validate()
			if len(tt.wantFields) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate() = %v, want *ValidationError", err)
			}
			got := map[string]int{}
			for _, f := range ve.Fields {
				got[f.Field]++
			}
			if len(ve.Fields) != len(tt.wantFields) {
				t.Fatalf("fields = %v, want %v", ve.Fields, tt.wantFields)
			}
			for _, f := range tt.wantFields {
				if got[f] != 1 {
					t.Errorf("want exactly one violation for %q in %v", f, ve.Fields)
				}
			}
		})
	}
}

func TestRequestJSONKeepsArabic(t *testing.T) {
	b, err := json.Marshal(validRequest())
	if err != nil {
		t.Fatal(err)
	}
	var back Request
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if got := string(back.Content[ChannelSMS]); got != `{"text":"مباراة الهلال والنصر تبدأ الساعة 8 مساءً"}` {
		t.Errorf("sms block changed: %s", got)
	}
}
