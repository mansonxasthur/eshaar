// Command sender consumes requests from Kafka, saves one pending row per
// channel, acknowledges, then sends each channel through its provider.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mansonxasthur/eshaar/cmd/internal/wiring"
	"github.com/mansonxasthur/eshaar/internal/adapters/kafka"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
	"github.com/mansonxasthur/eshaar/internal/sender"
)

func main() {
	log := logging.New().With("component", "sender")
	if err := run(log); err != nil {
		log.Error("sender stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv, config.NeedDatabase|config.NeedKafka|config.NeedSender)
	if err != nil {
		return err
	}
	// PROVIDER_RATE_LIMIT_RPS was replaced by the rps of each provider in the
	// providers file; a leftover value would otherwise be ignored without a word.
	if os.Getenv("PROVIDER_RATE_LIMIT_RPS") != "" {
		log.Warn("PROVIDER_RATE_LIMIT_RPS is no longer used; set rps per provider in the providers file", "file", cfg.ProvidersFile)
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
	// Log the effective order, since priorities can come from the environment.
	for _, c := range domain.Channels() {
		var names []string
		for _, r := range routes[c] {
			names = append(names, r.Provider.Name())
		}
		log.Info("provider chain", "channel", c, "providers", names)
	}
	return serve(ctx, cfg, log, routes)
}

// senderConfig maps the process configuration to the sender's.
func senderConfig(cfg config.Config) sender.Config {
	return sender.Config{
		PoolSize:      cfg.SenderPoolSize,
		SendTimeout:   cfg.SenderSendTimeout,
		StatusTimeout: cfg.ProviderStatusTimeout,
		// The longest send is the chain deadline, then a status check past it,
		// then the outcome write (5 s), with some margin.
		DrainTimeout: cfg.SenderSendTimeout + 10*time.Second,
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   5 * time.Second,
	}
}

// serve wires the adapters to the sender and runs it until ctx is done.
func serve(ctx context.Context, cfg config.Config, log *slog.Logger, providers map[domain.Channel][]sender.Route) error {
	store, err := postgres.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.SenderBatchSize, log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	s, err := sender.New(consumer, store, providers, senderConfig(cfg), log)
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "consuming", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID, "pool", cfg.SenderPoolSize)
	return s.Run(ctx)
}
