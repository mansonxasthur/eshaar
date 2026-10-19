package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		EnvDatabaseURL:  "postgres://u:p@localhost:5432/eshaar",
		EnvKafkaBrokers: "k1:9092, k2:9092,",
		EnvKafkaTopic:   "   ",
	}), NeedDatabase|NeedKafka|NeedHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"k1:9092", "k2:9092"}; !reflect.DeepEqual(cfg.KafkaBrokers, want) {
		t.Errorf("brokers = %v, want %v", cfg.KafkaBrokers, want)
	}
	if cfg.KafkaTopic != "notifications" || cfg.HTTPAddr != ":8080" {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		EnvDatabaseURL:  "postgresql://localhost/eshaar",
		EnvKafkaBrokers: "k1:9092",
		EnvKafkaTopic:   "events",
		EnvHTTPAddr:     "127.0.0.1:9000",
	}), NeedDatabase|NeedKafka|NeedHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaTopic != "events" || cfg.HTTPAddr != "127.0.0.1:9000" {
		t.Errorf("overrides ignored: %+v", cfg)
	}
}

func TestLoadOnlyChecksWhatIsNeeded(t *testing.T) {
	// The janitor needs Postgres only; Kafka and HTTP settings may be absent
	// or even malformed.
	_, err := Load(env(map[string]string{
		EnvDatabaseURL:  "postgres://localhost/eshaar",
		EnvKafkaBrokers: "garbage",
		EnvHTTPAddr:     "8080",
	}), NeedDatabase)
	if err != nil {
		t.Errorf("unneeded settings were validated: %v", err)
	}
}

func TestLoadReportsAllProblems(t *testing.T) {
	_, err := Load(env(map[string]string{EnvHTTPAddr: "8080"}), NeedDatabase|NeedKafka|NeedHTTP)
	if err == nil {
		t.Fatal("want error for empty environment")
	}
	for _, name := range []string{EnvDatabaseURL, EnvKafkaBrokers, EnvHTTPAddr} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not mention %s", err, name)
		}
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
		need Need
	}{
		{"mysql url", EnvDatabaseURL, "mysql://localhost/eshaar", NeedDatabase},
		{"no host", EnvDatabaseURL, "postgres://", NeedDatabase},
		{"blank url", EnvDatabaseURL, "   ", NeedDatabase},
		{"broker without port", EnvKafkaBrokers, "localhost", NeedKafka},
		{"broker bad port", EnvKafkaBrokers, "k1:abc", NeedKafka},
		{"broker without host", EnvKafkaBrokers, ":9092", NeedKafka},
		{"http without colon", EnvHTTPAddr, "8080", NeedHTTP},
		{"http port range", EnvHTTPAddr, ":70000", NeedHTTP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(env(map[string]string{tt.key: tt.val}), tt.need)
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Errorf("err = %v, want error mentioning %s", err, tt.key)
			}
		})
	}
}

func TestLoadDispatcherSettings(t *testing.T) {
	cfg, err := Load(env(map[string]string{EnvAPIKeys: "a, b,"}), NeedAPIKeys)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(cfg.APIKeys, want) {
		t.Errorf("keys = %v, want %v", cfg.APIKeys, want)
	}
	if cfg.RateLimitRPS != 1000 || cfg.RateLimitBurst != 2000 {
		t.Errorf("rate limit defaults = %v/%d", cfg.RateLimitRPS, cfg.RateLimitBurst)
	}

	cfg, err = Load(env(map[string]string{EnvAPIKeys: "a", EnvRateLimitRPS: "2.5", EnvRateLimitBurst: "7"}), NeedAPIKeys)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RateLimitRPS != 2.5 || cfg.RateLimitBurst != 7 {
		t.Errorf("rate limit overrides = %v/%d", cfg.RateLimitRPS, cfg.RateLimitBurst)
	}
}

func TestLoadDispatcherSettingsReportsEveryProblem(t *testing.T) {
	for _, bad := range []string{"NaN", "Inf"} {
		if _, err := Load(env(map[string]string{EnvAPIKeys: "a", EnvRateLimitRPS: bad}), NeedAPIKeys); err == nil {
			t.Errorf("%s accepted as a rate", bad)
		}
	}
	_, err := Load(env(map[string]string{EnvRateLimitRPS: "0", EnvRateLimitBurst: "x"}), NeedAPIKeys)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{EnvAPIKeys, EnvRateLimitRPS, EnvRateLimitBurst} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}

func TestLoadSenderDefaults(t *testing.T) {
	cfg, err := Load(env(nil), NeedSender)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaGroupID != "eshaar-sender" || cfg.SenderPoolSize != 256 || cfg.SenderBatchSize != 500 ||
		cfg.SenderSendTimeout != 4500*time.Millisecond || cfg.ProviderStatusTimeout != 500*time.Millisecond ||
		cfg.ProvidersFile != "infra/providers.yaml" {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestLoadSenderOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		EnvKafkaGroupID: "g2", EnvSenderPoolSize: "8", EnvSenderBatchSize: "50",
		EnvSenderSendTimeout: "1500ms", EnvProviderStatusTimeout: "90ms", EnvProvidersFile: "/etc/p.yaml",
	}), NeedSender)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaGroupID != "g2" || cfg.SenderPoolSize != 8 || cfg.SenderBatchSize != 50 ||
		cfg.SenderSendTimeout != 1500*time.Millisecond || cfg.ProviderStatusTimeout != 90*time.Millisecond ||
		cfg.ProvidersFile != "/etc/p.yaml" {
		t.Errorf("overrides ignored: %+v", cfg)
	}
}

func TestLoadSenderReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		EnvSenderPoolSize: "0", EnvSenderBatchSize: "x", EnvSenderSendTimeout: "soon", EnvProviderStatusTimeout: "-1s",
	}), NeedSender)
	if err == nil {
		t.Fatal("bad sender settings accepted")
	}
	for _, name := range []string{EnvSenderPoolSize, EnvSenderBatchSize, EnvSenderSendTimeout, EnvProviderStatusTimeout} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
	if _, err := Load(env(map[string]string{EnvSenderSendTimeout: "0s"}), NeedSender); err == nil {
		t.Error("zero send timeout accepted")
	}
	// Sender settings are not checked for a process that does not need them.
	if _, err := Load(env(map[string]string{EnvSenderPoolSize: "0"}), 0); err != nil {
		t.Errorf("unneeded settings were validated: %v", err)
	}
}

func TestLoadWebhookDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{EnvDatabaseURL: "postgres://localhost/eshaar"}), NeedDatabase|NeedHTTP|NeedWebhook)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebhookBatchSize != 200 || cfg.WebhookFlushers != 4 || cfg.WebhookResolveBatch != 200 ||
		cfg.WebhookQueueSize != 1600 || cfg.WebhookFlushInterval != 20*time.Millisecond ||
		cfg.WebhookFlushTimeout != 5*time.Second || cfg.WebhookResolveInterval != time.Second ||
		cfg.ProvidersFile != "infra/providers.yaml" {
		t.Errorf("defaults = %+v", cfg)
	}
}

func TestLoadWebhookOverridesAndQueueFollowsBatchAndFlushers(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		EnvDatabaseURL: "postgres://localhost/eshaar", EnvWebhookBatchSize: "10", EnvWebhookFlushers: "3",
		EnvWebhookFlushInterval: "50ms", EnvProvidersFile: "/etc/providers.yaml",
	}), NeedDatabase|NeedWebhook)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebhookBatchSize != 10 || cfg.WebhookQueueSize != 60 || cfg.WebhookFlushInterval != 50*time.Millisecond || cfg.ProvidersFile != "/etc/providers.yaml" {
		t.Errorf("config = %+v", cfg)
	}
	cfg, err = Load(env(map[string]string{EnvDatabaseURL: "postgres://localhost/eshaar", EnvWebhookQueueSize: "7"}), NeedDatabase|NeedWebhook)
	if err != nil || cfg.WebhookQueueSize != 7 {
		t.Errorf("explicit queue size ignored: %d, %v", cfg.WebhookQueueSize, err)
	}
}

func TestLoadWebhookRejectsBadSettings(t *testing.T) {
	_, err := Load(env(map[string]string{
		EnvDatabaseURL: "postgres://localhost/eshaar", EnvWebhookBatchSize: "0", EnvWebhookFlushers: "x",
		EnvWebhookFlushInterval: "-1s", EnvWebhookFlushTimeout: "soon", EnvWebhookResolveInterval: "0s", EnvWebhookResolveBatch: "-3",
	}), NeedDatabase|NeedWebhook)
	if err == nil {
		t.Fatal("bad settings accepted")
	}
	for _, name := range []string{EnvWebhookBatchSize, EnvWebhookFlushers, EnvWebhookFlushInterval, EnvWebhookFlushTimeout, EnvWebhookResolveInterval, EnvWebhookResolveBatch} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error does not mention %s: %v", name, err)
		}
	}
}

func TestLoadWebhookSettingsAreOnlyCheckedWhenNeeded(t *testing.T) {
	if _, err := Load(env(map[string]string{EnvDatabaseURL: "postgres://localhost/eshaar", EnvWebhookBatchSize: "garbage"}), NeedDatabase); err != nil {
		t.Errorf("unneeded webhook setting was validated: %v", err)
	}
}
