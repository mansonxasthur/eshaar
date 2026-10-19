// Package logging builds the process-wide slog logger.
//
// Records at INFO and below go to stdout, WARN and above go to stderr, so a
// container runtime can route the two streams independently.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// EnvLevel is the environment variable that selects the minimum log level.
const EnvLevel = "LOG_LEVEL"

// New returns a JSON logger writing to stdout and stderr, with the level taken
// from LOG_LEVEL (debug, info, warn, error; default info).
func New() *slog.Logger {
	return NewWithWriters(os.Stdout, os.Stderr, ParseLevel(os.Getenv(EnvLevel)))
}

// NewWithWriters is New with explicit sinks, for tests.
func NewWithWriters(stdout, stderr io.Writer, level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	return slog.New(&splitHandler{
		out: slog.NewJSONHandler(stdout, opts),
		err: slog.NewJSONHandler(stderr, opts),
	})
}

// ParseLevel maps a level name to a slog.Level, defaulting to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// splitHandler routes records below WARN to out and the rest to err.
type splitHandler struct {
	out, err slog.Handler
}

func (h *splitHandler) pick(l slog.Level) slog.Handler {
	if l >= slog.LevelWarn {
		return h.err
	}
	return h.out
}

func (h *splitHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.pick(l).Enabled(ctx, l)
}

func (h *splitHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.pick(r.Level).Handle(ctx, r)
}

func (h *splitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &splitHandler{out: h.out.WithAttrs(attrs), err: h.err.WithAttrs(attrs)}
}

func (h *splitHandler) WithGroup(name string) slog.Handler {
	return &splitHandler{out: h.out.WithGroup(name), err: h.err.WithGroup(name)}
}
