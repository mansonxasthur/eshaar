package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
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
			ve, ok := errors.AsType[*ValidationError](err)
			if !ok {
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

func TestRequestNotifications(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	req := validRequest()
	got := req.Notifications(now)

	if len(got) != 3 {
		t.Fatalf("got %d notifications, want 3", len(got))
	}
	wantOrder := []Channel{ChannelEmail, ChannelSMS, ChannelWhatsApp}
	for i, n := range got {
		if n.Channel != wantOrder[i] {
			t.Errorf("notification %d channel = %s, want %s", i, n.Channel, wantOrder[i])
		}
		if n.IdempotencyKey != "k1" || n.RecipientID != "u1" || n.NotificationType != "match_started" {
			t.Errorf("%s: request fields not copied: %+v", n.Channel, n)
		}
		if n.State != StatePending || !n.CreatedAt.Equal(now) || !n.UpdatedAt.Equal(now) {
			t.Errorf("%s: want pending at %v, got %s created %v", n.Channel, now, n.State, n.CreatedAt)
		}
		if string(n.Content) != string(req.Content[n.Channel]) {
			t.Errorf("%s: content = %s, want only its own block %s", n.Channel, n.Content, req.Content[n.Channel])
		}
	}
	if got[0].Email == nil || *got[0].Email != req.Email || got[0].Phone != nil {
		t.Errorf("email row contact fields wrong: %+v", got[0])
	}
	for _, n := range got[1:] {
		if n.Phone == nil || *n.Phone != req.Phone || n.Email != nil {
			t.Errorf("%s row contact fields wrong: %+v", n.Channel, n)
		}
	}
	if !strings.Contains(string(got[1].Content), "مباراة الهلال والنصر") {
		t.Errorf("Arabic text not kept as UTF-8: %s", got[1].Content)
	}
}

func TestRequestNotificationsSingleChannel(t *testing.T) {
	req := validRequest()
	delete(req.Content, ChannelEmail)
	delete(req.Content, ChannelWhatsApp)
	got := req.Notifications(time.Now())
	if len(got) != 1 || got[0].Channel != ChannelSMS {
		t.Fatalf("got %+v, want one sms notification", got)
	}
}
