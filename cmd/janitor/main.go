// Command janitor recovers pending notifications the sender left behind and
// marks the ones that never complete as unresolved.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mansonxasthur/eshaar/cmd/internal/wiring"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/janitor"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
	"github.com/mansonxasthur/eshaar/internal/sender"
)

func main() {
	log := logging.New().With("component", "janitor")
	if err := run(log); err != nil {
		log.Error("janitor stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv, config.NeedDatabase|config.NeedJanitor)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	specs, err := config.LoadProviders(cfg.ProvidersFile, os.Getenv)
	if err != nil {
		return err
	}
	routes, err := wiring.BuildRoutes(specs)
	if err != nil {
		return err
	}
	store, err := postgres.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()

	deliverer, err := sender.NewDeliverer(store, routes, sender.DelivererConfig{
		SendTimeout:   cfg.SenderSendTimeout,
		StatusTimeout: cfg.ProviderStatusTimeout,
	}, log)
	if err != nil {
		return err
	}
	j, err := janitor.New(store, deliverer, janitorConfig(cfg), log)
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "running", "interval", cfg.JanitorInterval, "sweeper_interval", cfg.SweeperInterval,
		"stale_after", cfg.JanitorStaleAfter, "batch", cfg.JanitorBatchSize, "concurrency", cfg.JanitorConcurrency)
	return j.Run(ctx)
}

// janitorConfig maps the process configuration to the janitor's.
func janitorConfig(cfg config.Config) janitor.Config {
	return janitor.Config{
		Interval:      cfg.JanitorInterval,
		SweepInterval: cfg.SweeperInterval,
		StaleAfter:    cfg.JanitorStaleAfter,
		BatchSize:     cfg.JanitorBatchSize,
		Concurrency:   cfg.JanitorConcurrency,
		// A re-drive asks the resume provider for its status, then runs the chain
		// under the send deadline, then may check status once more past it. The
		// outcome write (normally milliseconds) is not counted.
		RedriveBudget: cfg.SenderSendTimeout + 2*cfg.ProviderStatusTimeout,
	}
}
