package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/janitor"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
)

// The defaults must make a valid janitor: one re-drive has to fit in the stale
// age, or New refuses to start.
func TestDefaultsBuildAValidJanitorConfig(t *testing.T) {
	cfg, err := config.Load(func(k string) string {
		if k == config.EnvDatabaseURL {
			return "postgres://localhost/eshaar"
		}
		return ""
	}, config.NeedDatabase|config.NeedJanitor)
	if err != nil {
		t.Fatal(err)
	}
	jc := janitorConfig(cfg)
	if want := 5500 * time.Millisecond; jc.RedriveBudget != want {
		t.Errorf("budget = %v, want %v", jc.RedriveBudget, want)
	}
	if _, err := janitor.New(nil, nil, jc, nopLogger()); err != nil {
		t.Errorf("defaults rejected: %v", err)
	}
}

func nopLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }
