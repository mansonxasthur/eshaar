// Package config loads process settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Environment variable names.
const (
	EnvDatabaseURL  = "DATABASE_URL"
	EnvKafkaBrokers = "KAFKA_BROKERS"
	EnvKafkaTopic   = "KAFKA_TOPIC"
	EnvHTTPAddr     = "HTTP_ADDR"

	EnvAPIKeys        = "DISPATCHER_API_KEYS"
	EnvRateLimitRPS   = "RATE_LIMIT_RPS"
	EnvRateLimitBurst = "RATE_LIMIT_BURST"

	EnvKafkaGroupID      = "KAFKA_GROUP_ID"
	EnvSenderPoolSize    = "SENDER_POOL_SIZE"
	EnvSenderBatchSize   = "SENDER_BATCH_SIZE"
	EnvSenderSendTimeout = "SENDER_SEND_TIMEOUT"
	EnvProvidersFile     = "PROVIDERS_CONFIG"

	EnvProviderStatusTimeout = "PROVIDER_STATUS_TIMEOUT"

	EnvJanitorInterval    = "JANITOR_INTERVAL"
	EnvJanitorStaleAfter  = "JANITOR_STALE_AFTER"
	EnvJanitorBatchSize   = "JANITOR_BATCH_SIZE"
	EnvJanitorConcurrency = "JANITOR_CONCURRENCY"
	EnvSweeperInterval    = "SWEEPER_INTERVAL"

	EnvWebhookBatchSize       = "WEBHOOK_BATCH_SIZE"
	EnvWebhookFlushInterval   = "WEBHOOK_FLUSH_INTERVAL"
	EnvWebhookFlushers        = "WEBHOOK_FLUSHERS"
	EnvWebhookFlushTimeout    = "WEBHOOK_FLUSH_TIMEOUT"
	EnvWebhookQueueSize       = "WEBHOOK_QUEUE_SIZE"
	EnvWebhookResolveInterval = "WEBHOOK_RESOLVE_INTERVAL"
	EnvWebhookResolveBatch    = "WEBHOOK_RESOLVE_BATCH"
)

// Need selects the settings a process requires. Settings a process does not
// need are neither required nor validated, so the janitor can start without
// Kafka and the dispatcher without Postgres.
type Need uint

// The settings a process can require.
const (
	NeedDatabase Need = 1 << iota
	NeedKafka
	NeedHTTP
	NeedAPIKeys // API keys and the per-key rate limit
	NeedSender  // consumer group, send pool and outbound rate limit
	NeedWebhook // webhook batching and the providers file (for the secrets)
	NeedJanitor // recovery and sweeper timing, plus the providers file and send timeouts the re-drive shares with the sender
)

// Config holds the settings shared by the Eshaar processes.
type Config struct {
	DatabaseURL  string   // Postgres connection URL
	KafkaBrokers []string // seed brokers, host:port
	KafkaTopic   string   // topic the dispatcher publishes to and the sender consumes
	HTTPAddr     string   // listen address for processes that serve HTTP

	APIKeys        []string // accepted X-API-Key values (dispatcher)
	RateLimitRPS   float64  // requests per second allowed per API key, per instance
	RateLimitBurst int      // burst size per API key

	KafkaGroupID      string        // consumer group the senders join
	SenderPoolSize    int           // concurrent channel sends per sender
	SenderBatchSize   int           // most records read per poll
	SenderSendTimeout time.Duration // limit for a whole channel send, across all its providers

	JanitorInterval    time.Duration // time between recovery passes
	JanitorStaleAfter  time.Duration // age at which a pending row, or its last claim, counts as abandoned
	JanitorBatchSize   int           // rows claimed (or swept) per statement
	JanitorConcurrency int           // re-drives in flight at once; at least the batch size, so every claimed row starts at once
	SweeperInterval    time.Duration // time between sweeper passes

	ProvidersFile         string        // YAML file listing each channel's providers and their policies
	ProviderStatusTimeout time.Duration // limit for the status check after a provider times out

	WebhookBatchSize       int           // events saved by one statement
	WebhookFlushInterval   time.Duration // longest an event waits for its batch to fill
	WebhookFlushers        int           // batches saved concurrently
	WebhookFlushTimeout    time.Duration // limit for saving one batch
	WebhookQueueSize       int           // events waiting for a flusher; defaults to two batches per flusher
	WebhookResolveInterval time.Duration // how often events that beat their accept are re-resolved
	WebhookResolveBatch    int           // events linked per resolver pass
}

// Load reads the configuration through getenv (os.Getenv in production) and
// reports every missing or invalid setting among those in need at once.
func Load(getenv func(string) string, need Need) (Config, error) {
	cfg := Config{
		DatabaseURL: strings.TrimSpace(getenv(EnvDatabaseURL)),
		KafkaTopic:  valueOr(getenv(EnvKafkaTopic), "notifications"),
		HTTPAddr:    valueOr(getenv(EnvHTTPAddr), ":8080"),
	}
	cfg.KafkaBrokers = splitList(getenv(EnvKafkaBrokers))

	cfg.APIKeys = splitList(getenv(EnvAPIKeys))

	var errs []error
	if need&NeedAPIKeys != 0 {
		if len(cfg.APIKeys) == 0 {
			errs = append(errs, fmt.Errorf("%s is required (comma-separated)", EnvAPIKeys))
		}
		rps, err := strconv.ParseFloat(valueOr(getenv(EnvRateLimitRPS), "1000"), 64)
		if err != nil || math.IsNaN(rps) || math.IsInf(rps, 0) || rps <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive number", EnvRateLimitRPS))
		}
		burst, err := strconv.Atoi(valueOr(getenv(EnvRateLimitBurst), "2000"))
		if err != nil || burst < 1 {
			errs = append(errs, fmt.Errorf("%s must be an integer of at least 1", EnvRateLimitBurst))
		}
		cfg.RateLimitRPS, cfg.RateLimitBurst = rps, burst
	}
	var err error
	if need&NeedSender != 0 {
		cfg.KafkaGroupID = valueOr(getenv(EnvKafkaGroupID), "eshaar-sender")
		if cfg.SenderPoolSize, err = positiveInt(getenv, EnvSenderPoolSize, "256"); err != nil {
			errs = append(errs, err)
		}
		if cfg.SenderBatchSize, err = positiveInt(getenv, EnvSenderBatchSize, "500"); err != nil {
			errs = append(errs, err)
		}
	}
	// The janitor re-drives through the same chains as the sender, so it reads
	// the same chain deadline and status timeout.
	if need&(NeedSender|NeedJanitor) != 0 {
		for _, d := range []struct {
			dst  *time.Duration
			name string
			def  string
		}{
			{&cfg.SenderSendTimeout, EnvSenderSendTimeout, "4500ms"},
			{&cfg.ProviderStatusTimeout, EnvProviderStatusTimeout, "500ms"},
		} {
			if *d.dst, err = positiveDuration(getenv, d.name, d.def); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if need&NeedJanitor != 0 {
		if cfg.JanitorBatchSize, err = positiveInt(getenv, EnvJanitorBatchSize, "50"); err != nil {
			errs = append(errs, err)
		}
		if cfg.JanitorConcurrency, err = positiveInt(getenv, EnvJanitorConcurrency, "50"); err != nil {
			errs = append(errs, err)
		}
		for _, d := range []struct {
			dst  *time.Duration
			name string
			def  string
		}{
			{&cfg.JanitorInterval, EnvJanitorInterval, "5s"},
			{&cfg.JanitorStaleAfter, EnvJanitorStaleAfter, "10s"},
			{&cfg.SweeperInterval, EnvSweeperInterval, "10m"},
		} {
			if *d.dst, err = positiveDuration(getenv, d.name, d.def); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if need&(NeedSender|NeedWebhook|NeedJanitor) != 0 {
		cfg.ProvidersFile = valueOr(getenv(EnvProvidersFile), "infra/providers.yaml")
	}
	if need&NeedWebhook != 0 {
		if cfg.WebhookBatchSize, err = positiveInt(getenv, EnvWebhookBatchSize, "200"); err != nil {
			errs = append(errs, err)
		}
		if cfg.WebhookFlushers, err = positiveInt(getenv, EnvWebhookFlushers, "4"); err != nil {
			errs = append(errs, err)
		}
		if cfg.WebhookResolveBatch, err = positiveInt(getenv, EnvWebhookResolveBatch, "200"); err != nil {
			errs = append(errs, err)
		}
		queueDefault := strconv.Itoa(max(1, 2*cfg.WebhookBatchSize*cfg.WebhookFlushers))
		if cfg.WebhookQueueSize, err = positiveInt(getenv, EnvWebhookQueueSize, queueDefault); err != nil {
			errs = append(errs, err)
		}
		for _, d := range []struct {
			dst  *time.Duration
			name string
			def  string
		}{
			{&cfg.WebhookFlushInterval, EnvWebhookFlushInterval, "20ms"},
			{&cfg.WebhookFlushTimeout, EnvWebhookFlushTimeout, "5s"},
			{&cfg.WebhookResolveInterval, EnvWebhookResolveInterval, "1s"},
		} {
			if *d.dst, err = positiveDuration(getenv, d.name, d.def); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if need&NeedDatabase != 0 {
		errs = append(errs, checkDatabaseURL(cfg.DatabaseURL))
	}
	if need&NeedKafka != 0 {
		if len(cfg.KafkaBrokers) == 0 {
			errs = append(errs, fmt.Errorf("%s is required (comma-separated host:port)", EnvKafkaBrokers))
		}
		for _, b := range cfg.KafkaBrokers {
			if err := checkHostPort(b, true); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", EnvKafkaBrokers, err))
			}
		}
	}
	if need&NeedHTTP != 0 {
		if err := checkHostPort(cfg.HTTPAddr, false); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", EnvHTTPAddr, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// positiveDuration reads a duration above zero, using def when unset.
func positiveDuration(getenv func(string) string, name, def string) (time.Duration, error) {
	d, err := time.ParseDuration(valueOr(getenv(name), def))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as %s", name, def)
	}
	return d, nil
}

// positiveInt reads an integer of at least 1, using def when unset.
func positiveInt(getenv func(string) string, name, def string) (int, error) {
	n, err := strconv.Atoi(valueOr(getenv(name), def))
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be an integer of at least 1", name)
	}
	return n, nil
}

func checkDatabaseURL(s string) error {
	if s == "" {
		return fmt.Errorf("%s is required", EnvDatabaseURL)
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		return fmt.Errorf("%s must be a postgres://host/db URL", EnvDatabaseURL)
	}
	return nil
}

// checkHostPort validates addr as host:port. The host may be empty (listen on
// all interfaces) unless needHost is set.
func checkHostPort(addr string, needHost bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q is not host:port", addr)
	}
	if needHost && host == "" {
		return fmt.Errorf("%q has no host", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%q has an invalid port", addr)
	}
	return nil
}

// splitList splits a comma-separated list and drops blank items.
func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func valueOr(v, def string) string {
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return def
}
