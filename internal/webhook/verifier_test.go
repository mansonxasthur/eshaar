package webhook

import (
	"errors"
	"strings"
	"testing"

	"github.com/mansonxasthur/eshaar/internal/ports"
)

const secretA = "0123456789abcdef"

func newVerifier(t *testing.T, secrets map[string]string) *Verifier {
	t.Helper()
	v, err := NewVerifier(secrets)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerifyAcceptsAValidSignature(t *testing.T) {
	v := newVerifier(t, map[string]string{"sms-a": secretA})
	body := []byte(`{"message_id":"m1"}`)
	if err := v.Verify("sms-a", body, Sign(secretA, body)); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := v.Verify("sms-a", body, strings.ToUpper(Sign(secretA, body))); err != nil {
		t.Errorf("upper-case hex rejected: %v", err)
	}
}

func TestVerifyRejectsBadSignatures(t *testing.T) {
	v := newVerifier(t, map[string]string{"sms-a": secretA, "sms-b": "fedcba9876543210"})
	body := []byte(`{"message_id":"m1"}`)
	good := Sign(secretA, body)
	cases := map[string]string{
		"empty":            "",
		"not hex":          "zz" + good[2:],
		"truncated":        good[:len(good)-2],
		"odd length":       good[:len(good)-1],
		"other body":       Sign(secretA, []byte(`{"message_id":"m2"}`)),
		"other provider":   Sign("fedcba9876543210", body),
		"with prefix":      "sha256=" + good,
		"trailing newline": good + "\n",
	}
	for name, sig := range cases {
		if err := v.Verify("sms-a", body, sig); !errors.Is(err, ports.ErrInvalidSignature) {
			t.Errorf("%s: err = %v, want ErrInvalidSignature", name, err)
		}
	}
}

func TestVerifyUnknownProvider(t *testing.T) {
	v := newVerifier(t, map[string]string{"sms-a": secretA})
	body := []byte(`{}`)
	err := v.Verify("nope", body, Sign(secretA, body))
	if !errors.Is(err, ports.ErrUnknownProvider) {
		t.Errorf("err = %v, want ErrUnknownProvider", err)
	}
	if strings.Contains(err.Error(), "nope") {
		t.Errorf("error echoes the caller's provider name: %v", err)
	}
}

func TestEmptySecretNeverVerifies(t *testing.T) {
	v := newVerifier(t, map[string]string{"sms-a": "", "sms-b": secretA})
	if v.Len() != 1 {
		t.Errorf("Len = %d, want 1: a provider without a secret must be left out", v.Len())
	}
	body := []byte(`{}`)
	// A signature made with an empty key is exactly what a forger would send.
	if err := v.Verify("sms-a", body, Sign("", body)); !errors.Is(err, ports.ErrUnknownProvider) {
		t.Errorf("empty-key signature: err = %v, want ErrUnknownProvider", err)
	}
	// And the placeholder key used for unknown providers must not verify either.
	if err := v.Verify("nope", body, Sign(unknownSecret, body)); err == nil {
		t.Error("signature made with the unknown-provider key was accepted")
	}
}

func TestSignIsHMACSHA256Hex(t *testing.T) {
	// RFC 4231 test case 2.
	got := Sign("Jefe", []byte("what do ya want for nothing?"))
	const want = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
	if got != want {
		t.Errorf("Sign = %s, want %s", got, want)
	}
}

func TestNewVerifierRejectsShortSecretsWithoutShowingThem(t *testing.T) {
	_, err := NewVerifier(map[string]string{"sms-a": "short-secret", "sms-b": secretA, "email-a": "tiny"})
	if err == nil {
		t.Fatal("short secrets accepted")
	}
	if !strings.Contains(err.Error(), "email-a, sms-a") || strings.Contains(err.Error(), "short-secret") || strings.Contains(err.Error(), "tiny") {
		t.Errorf("error = %v", err)
	}
	if _, err := NewVerifier(map[string]string{"a": strings.Repeat("x", MinSecretLength-1)}); err == nil {
		t.Error("a secret one character short was accepted")
	}
	if _, err := NewVerifier(map[string]string{"a": strings.Repeat("x", MinSecretLength)}); err != nil {
		t.Errorf("a secret of the minimum length was rejected: %v", err)
	}
}
