package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

// encode returns the Kafka record value for req: JSON with non-ASCII text kept
// as UTF-8, so Arabic content is never escaped.
func encode(req domain.Request) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// decode parses a record value written by encode.
func decode(value []byte) (domain.Request, error) {
	var req domain.Request
	if err := json.Unmarshal(value, &req); err != nil {
		return domain.Request{}, fmt.Errorf("decode request: %w", err)
	}
	return req, nil
}
