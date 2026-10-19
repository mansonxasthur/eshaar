// Command webhook receives provider events, checks their signatures, stores
// them and records each message's final delivery state. It never triggers a
// fallback.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/mansonxasthur/eshaar/internal/adapters/httpapi"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
	"github.com/mansonxasthur/eshaar/internal/webhook"
)

// shutdownTimeout bounds how long the HTTP server waits for requests in flight.
const shutdownTimeout = 15 * time.Second

func main() {
	log := logging.New().With("component", "webhook")
	if err := run(log); err != nil {
		log.Error("webhook stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv, config.NeedDatabase|config.NeedHTTP|config.NeedWebhook)
	if err != nil {
		return err
	}
	specs, err := config.LoadProviders(cfg.ProvidersFile, os.Getenv)
	if err != nil {
		return err
	}
	providers, err := webhookProviders(specs)
	if err != nil {
		return err
	}
	if err := checkProviders(log, providers); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store, err := postgres.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	svc, err := webhook.New(store, providers, webhookConfig(cfg), log)
	if err != nil {
		return err
	}
	return serve(ctx, cfg.HTTPAddr, svc, log)
}

// serve runs the service and the HTTP server until ctx is done or one fails.
// It stops them in order: the server first, so no request is still waiting on
// the service, then the service, which saves what is queued.
func serve(ctx context.Context, addr string, svc *webhook.Service, log *slog.Logger) error {
	srv := httpapi.NewServer(addr, httpapi.NewWebhookHandler(svc, log).Routes())

	svcCtx, cancelSvc := context.WithCancel(context.Background())
	defer cancelSvc()
	svcDone := make(chan error, 1)
	go func() { svcDone <- svc.Run(svcCtx) }()

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()
	log.InfoContext(ctx, "listening", "addr", addr)

	var runErr error
	svcExited := false
	select {
	case err := <-srvErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	case err := <-svcDone:
		svcExited = true
		runErr = errors.Join(errors.New("webhook service stopped"), err)
	case <-ctx.Done():
	}
	log.InfoContext(ctx, "shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		runErr = errors.Join(runErr, err)
	}
	cancelSvc()
	if !svcExited { // its result was already read above
		if err := <-svcDone; err != nil && !errors.Is(err, context.Canceled) {
			runErr = errors.Join(runErr, err)
		}
	}
	return runErr
}

// webhookConfig maps the process configuration to the service's.
func webhookConfig(cfg config.Config) webhook.Config {
	return webhook.Config{
		BatchSize:       cfg.WebhookBatchSize,
		FlushInterval:   cfg.WebhookFlushInterval,
		Flushers:        cfg.WebhookFlushers,
		FlushTimeout:    cfg.WebhookFlushTimeout,
		QueueSize:       cfg.WebhookQueueSize,
		ResolveInterval: cfg.WebhookResolveInterval,
		ResolveBatch:    cfg.WebhookResolveBatch,
	}
}

// webhookProviders builds, from the providers file, what the service needs to
// know about each provider: its secret and the channels it serves. A provider
// listed under several channels serves all of them, and must carry the same
// secret under each, since a webhook names only the provider.
func webhookProviders(specs config.Providers) (map[string]webhook.Provider, error) {
	channels := make([]string, 0, len(specs))
	for c := range specs {
		channels = append(channels, c)
	}
	sort.Strings(channels)

	out := make(map[string]webhook.Provider)
	for _, c := range channels {
		channel, err := domain.ParseChannel(c)
		if err != nil {
			return nil, fmt.Errorf("providers: %w", err)
		}
		for _, spec := range specs[c] {
			p := out[spec.Name]
			if len(p.Channels) > 0 && p.Secret != spec.WebhookSecret.Reveal() {
				return nil, fmt.Errorf("providers: %s has different webhook secrets under different channels", spec.Name)
			}
			p.Secret = spec.WebhookSecret.Reveal()
			p.Channels = append(p.Channels, channel)
			out[spec.Name] = p
		}
	}
	return out, nil
}

// checkProviders logs which providers can deliver webhooks and which cannot, by
// name only, and fails when none can, before anything connects: a missing
// secret is the likely cause of refused events, and a process that refuses
// every webhook should not look healthy.
func checkProviders(log *slog.Logger, providers map[string]webhook.Provider) error {
	var enabled, disabled []string
	for name, p := range providers {
		if p.Secret == "" {
			disabled = append(disabled, name)
		} else {
			enabled = append(enabled, name)
		}
	}
	sort.Strings(enabled)
	sort.Strings(disabled)
	if len(enabled) == 0 {
		return errors.New("no provider has a webhook secret; set <PROVIDER>_WEBHOOK_SECRET variables named in the providers file")
	}
	log.Info("webhook providers", "with_secret", enabled)
	if len(disabled) > 0 {
		log.Warn("providers without a webhook secret cannot deliver webhooks", "providers", disabled)
	}
	return nil
}
