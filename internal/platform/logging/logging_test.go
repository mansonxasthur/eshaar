package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestRoutesByLevel(t *testing.T) {
	var stdout, stderr bytes.Buffer
	log := NewWithWriters(&stdout, &stderr, slog.LevelDebug)

	log.Debug("d")
	log.Info("i")
	log.Warn("w")
	log.Error("e")

	for _, msg := range []string{`"msg":"d"`, `"msg":"i"`} {
		if !strings.Contains(stdout.String(), msg) {
			t.Errorf("stdout missing %s: %q", msg, stdout.String())
		}
	}
	for _, msg := range []string{`"msg":"w"`, `"msg":"e"`} {
		if !strings.Contains(stderr.String(), msg) {
			t.Errorf("stderr missing %s: %q", msg, stderr.String())
		}
	}
	if strings.Contains(stdout.String(), `"msg":"w"`) || strings.Contains(stderr.String(), `"msg":"i"`) {
		t.Errorf("record routed to the wrong stream: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestLevelFilter(t *testing.T) {
	var stdout, stderr bytes.Buffer
	log := NewWithWriters(&stdout, &stderr, slog.LevelWarn)

	log.Info("hidden")
	if stdout.Len() != 0 {
		t.Errorf("info logged at warn level: %q", stdout.String())
	}
}

func TestWithAttrsKeepsRouting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	log := NewWithWriters(&stdout, &stderr, slog.LevelInfo).With("component", "sender")

	log.Error("boom")
	if !strings.Contains(stderr.String(), `"component":"sender"`) {
		t.Errorf("attrs lost: %q", stderr.String())
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelInfo,
		"info":    slog.LevelInfo,
		"DEBUG":   slog.LevelDebug,
		" warn ":  slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
