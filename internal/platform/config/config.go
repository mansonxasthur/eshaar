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
	EnvProviderRateLimit = "PROVIDER_RATE_LIMIT_RPS"

	EnvProviderATimeout      = "PROVIDER_A_TIMEOUT"
	EnvProviderBTimeout      = "PROVIDER_B_TIMEOUT"
	EnvProviderStatusTimeout = "PROVIDER_STATUS_TIMEOUT"
	EnvBreakerFailures       = "PROVIDER_BREAKER_FAILURES"
	EnvBreakerOpenFor        = "PROVIDER_BREAKER_OPEN_FOR"
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
	SenderSendTimeout time.Duration // limit for a whole channel send, across both providers
	ProviderRateLimit float64       // provider calls per second per sender

	ProviderATimeout      time.Duration // limit for one call to provider A
	ProviderBTimeout      time.Duration // limit for one call to provider B
	ProviderStatusTimeout time.Duration // limit for the status check after A times out
	BreakerFailures       int           // consecutive failures that open a provider's circuit breaker
	BreakerOpenFor        time.Duration // how long an open breaker waits before probing
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
	if need&NeedSender != 0 {
		cfg.KafkaGroupID = valueOr(getenv(EnvKafkaGroupID), "eshaar-sender")
		var err error
		if cfg.SenderPoolSize, err = positiveInt(getenv, EnvSenderPoolSize, "256"); err != nil {
			errs = append(errs, err)
		}
		if cfg.SenderBatchSize, err = positiveInt(getenv, EnvSenderBatchSize, "500"); err != nil {
			errs = append(errs, err)
		}
		for _, d := range []struct {
			dst  *time.Duration
			name string
			def  string
		}{
			{&cfg.SenderSendTimeout, EnvSenderSendTimeout, "4500ms"},
			{&cfg.ProviderATimeout, EnvProviderATimeout, "2s"},
			{&cfg.ProviderBTimeout, EnvProviderBTimeout, "2s"},
			{&cfg.ProviderStatusTimeout, EnvProviderStatusTimeout, "500ms"},
			{&cfg.BreakerOpenFor, EnvBreakerOpenFor, "30s"},
		} {
			if *d.dst, err = positiveDuration(getenv, d.name, d.def); err != nil {
				errs = append(errs, err)
			}
		}
		if cfg.BreakerFailures, err = positiveInt(getenv, EnvBreakerFailures, "5"); err != nil {
			errs = append(errs, err)
		}
		cfg.ProviderRateLimit, err = strconv.ParseFloat(valueOr(getenv(EnvProviderRateLimit), "1000"), 64)
		if err != nil || math.IsNaN(cfg.ProviderRateLimit) || math.IsInf(cfg.ProviderRateLimit, 0) || cfg.ProviderRateLimit <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive number", EnvProviderRateLimit))
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
