package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const providersYAML = `
sms:
  - name: sms-a
    priority: ${SMS_A_PRIORITY:-1}
    rps: ${SMS_A_RPS:-200}
    timeout: ${SMS_A_TIMEOUT:-2s}
    breaker: {failures: 5, open_for: 30s}
  - name: sms-b
    priority: 2
    kind: mock
    rps: 50.5
    timeout: 1500ms
    breaker: {failures: 3, open_for: 10s}
  - name: sms-c
    priority: 3
    rps: 10
    timeout: 1s
    breaker: {failures: 1, open_for: 5s}
email:
  - name: email-a
    priority: 1
    rps: ${EMAIL_RPS}
    timeout: 2s
    breaker: {failures: 5, open_for: 30s}
`

func TestParseProvidersReadsOrderedChainsAndEnvValues(t *testing.T) {
	p, err := ParseProviders([]byte(providersYAML), env(map[string]string{"SMS_A_TIMEOUT": "750ms", "EMAIL_RPS": "99"}))
	if err != nil {
		t.Fatal(err)
	}
	sms := p["sms"]
	if len(sms) != 3 || sms[0].Name != "sms-a" || sms[1].Name != "sms-b" || sms[2].Name != "sms-c" {
		t.Fatalf("sms chain = %+v", sms)
	}
	if sms[0].RPS != 200 || sms[0].Timeout != 750*time.Millisecond {
		t.Errorf("sms-a = %+v, want default rps and env timeout", sms[0])
	}
	if sms[1].RPS != 50.5 || sms[1].Timeout != 1500*time.Millisecond || sms[1].Breaker.Failures != 3 || sms[1].Breaker.OpenFor != 10*time.Second {
		t.Errorf("sms-b = %+v", sms[1])
	}
	if p["email"][0].RPS != 99 {
		t.Errorf("email rps = %v, want the env value", p["email"][0].RPS)
	}
}

func TestParseProvidersReportsEveryProblem(t *testing.T) {
	const bad = `
sms:
  - name: ""
    rps: 0
    timeout: 0s
    breaker: {failures: 0, open_for: 0s}
  - name: dup
    priority: 1
    rps: 1
    timeout: 1s
    breaker: {failures: 1, open_for: 1s}
  - name: dup
    priority: 1
    rps: 1
    timeout: 1s
    breaker: {failures: 1, open_for: 1s}
email: []
`
	_, err := ParseProviders([]byte(bad), env(nil))
	if err == nil {
		t.Fatal("bad providers accepted")
	}
	for _, want := range []string{
		"sms[0]: name is required", "sms[0]: provider \"\": priority must be at least 1, got 0", `sms[2]: provider "dup": priority 1 is already used`, "sms[0]: rps", "sms[0]: timeout", "sms[0]: breaker.failures", "sms[0]: breaker.open_for",
		`sms[2]: provider "dup" is listed twice`, "email: at least one provider",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestParseProvidersRejectsBadInput(t *testing.T) {
	good := "sms:\n  - {name: a, priority: 1, rps: 1, timeout: 1s, breaker: {failures: 1, open_for: 1s}}\n"
	cases := map[string]string{
		"unset variable":        strings.Replace(good, "rps: 1", "rps: ${MISSING}", 1),
		"unknown field":         strings.Replace(good, "name: a,", "name: a, color: red,", 1),
		"not a duration":        strings.Replace(good, "timeout: 1s", "timeout: soon", 1),
		"no channels":           "",
		"not yaml":              "sms: [",
		"empty env, no default": strings.Replace(good, "rps: 1", "rps: ${EMPTY}", 1),
		"yaml injection":        strings.Replace(good, "timeout: 1s", "timeout: ${INJECT}", 1),
		"unsafe default":        strings.Replace(good, "timeout: 1s", "timeout: ${X:-1s, name: y}", 1),
		"comment-only file":     "# nothing here\n",
		"bare number timeout":   strings.Replace(good, "timeout: 1s", "timeout: 2", 1),
	}
	for name, in := range cases {
		if _, err := ParseProviders([]byte(in), env(map[string]string{"EMPTY": "", "INJECT": "2s}\n  - name: x"})); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ParseProviders([]byte(good), env(nil)); err != nil {
		t.Errorf("good file rejected: %v", err)
	}
	_, err := ParseProviders([]byte(cases["unset variable"]), env(nil))
	if err == nil || !strings.Contains(err.Error(), "MISSING") {
		t.Errorf("error does not name the variable: %v", err)
	}
}

func TestLoadProvidersReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "providers.yaml")
	if err := os.WriteFile(path, []byte(providersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProviders(path, env(map[string]string{"EMAIL_RPS": "1"})); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProviders(filepath.Join(t.TempDir(), "missing.yaml"), env(nil)); err == nil {
		t.Error("missing file accepted")
	}
}

func TestParseProvidersSortsChainByPriority(t *testing.T) {
	const yamlText = `
sms:
  - {name: third, priority: 30, rps: 1, timeout: 1s, breaker: {failures: 1, open_for: 1s}}
  - {name: first, priority: ${FIRST:-1}, rps: 1, timeout: 1s, breaker: {failures: 1, open_for: 1s}}
  - {name: second, priority: 2, rps: 1, timeout: 1s, breaker: {failures: 1, open_for: 1s}}
`
	p, err := ParseProviders([]byte(yamlText), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, pr := range p["sms"] {
		got = append(got, pr.Name)
	}
	if strings.Join(got, ",") != "first,second,third" {
		t.Errorf("chain = %v, want it ordered by priority, not by file order", got)
	}

	// An environment value can reorder the chain without editing the file.
	p, err = ParseProviders([]byte(yamlText), env(map[string]string{"FIRST": "99"}))
	if err != nil {
		t.Fatal(err)
	}
	if p["sms"][0].Name != "second" || p["sms"][2].Name != "first" {
		t.Errorf("chain = %+v, want first demoted to the end", p["sms"])
	}
}
