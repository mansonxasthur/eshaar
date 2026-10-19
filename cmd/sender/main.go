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

	"github.com/mansonxasthur/eshaar/internal/adapters/kafka"
	"github.com/mansonxasthur/eshaar/internal/adapters/mock"
	"github.com/mansonxasthur/eshaar/internal/adapters/postgres"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/platform/logging"
	"github.com/mansonxasthur/eshaar/internal/ports"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, log, defaultProviders(cfg))
}

// defaultProviders sends every channel through the mock provider, behind the
// outbound rate limit. Phase 4 replaces it with a provider A and B per channel.
func defaultProviders(cfg config.Config) map[domain.Channel]ports.Provider {
	p := sender.WithRateLimit(mock.NewProvider("mock"), cfg.ProviderRateLimit)
	providers := make(map[domain.Channel]ports.Provider)
	for _, c := range domain.Channels() {
		providers[c] = p
	}
	return providers
}

// serve wires the adapters to the sender and runs it until ctx is done.
func serve(ctx context.Context, cfg config.Config, log *slog.Logger, providers map[domain.Channel]ports.Provider) error {
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

	s, err := sender.New(consumer, store, providers, sender.Config{
		PoolSize:     cfg.SenderPoolSize,
		SendTimeout:  cfg.SenderSendTimeout,
		DrainTimeout: cfg.SenderSendTimeout + 5*time.Second,
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   5 * time.Second,
	}, log)
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "consuming", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID, "pool", cfg.SenderPoolSize)
	return s.Run(ctx)
}
