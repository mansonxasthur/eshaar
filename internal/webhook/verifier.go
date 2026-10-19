// Package webhook checks and parses the events providers send about messages
// they accepted, and stores them. It imports only the domain and the ports.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/mansonxasthur/eshaar/internal/ports"
)

// Sign returns the signature a provider puts on body: the hex of an
// HMAC-SHA256 over the raw body, keyed with the provider's secret. The verifier
// and the tests that play a provider (the end-to-end test of cmd/webhook) both
// use it, so the scheme lives in one place.
func Sign(secret string, body []byte) string {
	return hex.EncodeToString(sum(secret, body))
}

func sum(secret string, body []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return mac.Sum(nil)
}

// Verifier checks webhook signatures with the secret of each provider.
type Verifier struct {
	secrets map[string]string
}

// MinSecretLength is the shortest webhook secret accepted, so a placeholder
// value like "x" cannot pass for a key.
const MinSecretLength = 16

// NewVerifier returns a verifier for the given secrets, by provider name. A
// provider with an empty secret is left out, so it counts as unknown: an empty
// key would let anyone sign. A secret shorter than MinSecretLength is an error
// naming the providers, never the secrets. It fails for all providers when one
// secret is short, so a typo shows at startup and not as lost webhooks.
func NewVerifier(secrets map[string]string) (*Verifier, error) {
	v := &Verifier{secrets: make(map[string]string, len(secrets))}
	var short []string
	for provider, secret := range secrets {
		switch {
		case secret == "":
		case len(secret) < MinSecretLength:
			short = append(short, provider)
		default:
			v.secrets[provider] = secret
		}
	}
	if len(short) > 0 {
		sort.Strings(short)
		return nil, fmt.Errorf("webhook secret shorter than %d characters for provider(s): %s", MinSecretLength, strings.Join(short, ", "))
	}
	return v, nil
}

// Len returns how many providers can deliver webhooks.
func (v *Verifier) Len() int { return len(v.secrets) }

// unknownSecret is keyed against for a provider that is not configured, so an
// unknown provider costs the same time as a wrong signature and the response
// time does not say which providers exist.
const unknownSecret = "unknown-provider"

// Verify checks signature, the hex HMAC of body, against provider's secret. It
// returns ports.ErrInvalidSignature for a wrong, malformed or missing signature
// and ports.ErrUnknownProvider for a provider with no secret, both bare so the
// caller's provider name is not echoed into logs by the error. The HTTP adapter
// answers both the same way.
func (v *Verifier) Verify(provider string, body []byte, signature string) error {
	secret, known := v.secrets[provider]
	if !known {
		secret = unknownSecret
	}
	got, err := hex.DecodeString(signature)
	ok := err == nil && hmac.Equal(got, sum(secret, body))
	switch {
	case !known:
		return ports.ErrUnknownProvider
	case !ok:
		return ports.ErrInvalidSignature
	}
	return nil
}
