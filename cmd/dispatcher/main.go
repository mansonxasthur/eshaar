// Command dispatcher is the HTTP entry point: it authenticates, rate-limits and
// validates a request, publishes it to Kafka and answers 202 once Kafka confirms.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mansonxasthur/eshaar/internal/adapters/httpapi"
	"github.com/mansonxasthur/eshaar/internal/adapters/kafka"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

func main() {
	log := logging.New().With("component", "dispatcher")
	if err := run(log); err != nil {
		log.Error("dispatcher stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv, config.NeedKafka|config.NeedHTTP|config.NeedAPIKeys)
	if err != nil {
		return err
	}
	pub, err := kafka.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return err
	}
	defer pub.Close()
	handler, err := routes(cfg, pub, log)
	if err != nil {
		return err
	}
	srv := httpapi.NewServer(cfg.HTTPAddr, handler)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("listening", "addr", cfg.HTTPAddr, "topic", cfg.KafkaTopic)

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// routes wires the handler behind API key auth and the per-key rate limit.
func routes(cfg config.Config, pub ports.Publisher, log *slog.Logger) (http.Handler, error) {
	limiter, err := httpapi.NewRateLimiter(cfg.APIKeys, cfg.RateLimitRPS, cfg.RateLimitBurst)
	if err != nil {
		return nil, err
	}
	return httpapi.NewHandler(pub, log).Routes(httpapi.APIKeyAuth(cfg.APIKeys), limiter.Middleware), nil
}
