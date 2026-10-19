package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// body builds the JSON for request number seq of a run. Every request has a
// unique idempotency key (run id plus sequence), and carries all channels in
// channels, so one request makes len(channels) notification rows.
func body(run string, seq int64, channels []string) ([]byte, error) {
	content := map[string]any{}
	for _, c := range channels {
		switch c {
		case "sms":
			content[c] = map[string]string{"text": "load test message"}
		case "email", "whatsapp":
			content[c] = map[string]string{"template_id": "load-test"}
		default:
			return nil, fmt.Errorf("unknown channel %q", c)
		}
	}
	req := map[string]any{
		"idempotency_key":   fmt.Sprintf("%s-%d", run, seq),
		"recipient_id":      fmt.Sprintf("user-%d", seq%100000),
		"notification_type": "load_test",
		"content":           content,
	}
	if has(channels, "email") {
		req["email"] = "user@example.com"
	}
	if has(channels, "sms") || has(channels, "whatsapp") {
		req["phone"] = "+966500000000"
	}
	return json.Marshal(req)
}

func has(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
