// Package config loads process settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Environment variable names.
const (
	EnvDatabaseURL  = "DATABASE_URL"
	EnvKafkaBrokers = "KAFKA_BROKERS"
	EnvKafkaTopic   = "KAFKA_TOPIC"
	EnvHTTPAddr     = "HTTP_ADDR"
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
)

// Config holds the settings shared by the Eshaar processes.
type Config struct {
	DatabaseURL  string   // Postgres connection URL
	KafkaBrokers []string // seed brokers, host:port
	KafkaTopic   string   // topic the dispatcher publishes to and the sender consumes
	HTTPAddr     string   // listen address for processes that serve HTTP
}

// Load reads the configuration through getenv (os.Getenv in production) and
// reports every missing or invalid setting among those in need at once.
func Load(getenv func(string) string, need Need) (Config, error) {
	cfg := Config{
		DatabaseURL: strings.TrimSpace(getenv(EnvDatabaseURL)),
		KafkaTopic:  valueOr(getenv(EnvKafkaTopic), "notifications"),
		HTTPAddr:    valueOr(getenv(EnvHTTPAddr), ":8080"),
	}
	for _, b := range strings.Split(getenv(EnvKafkaBrokers), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, b)
		}
	}

	var errs []error
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

func valueOr(v, def string) string {
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return def
}
