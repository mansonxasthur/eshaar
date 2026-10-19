package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Providers lists, per channel, the providers to try in order of priority: the
// lowest number is the primary and each higher one is a fallback for the one
// before it. ParseProviders returns each channel's list sorted that way. Every
// channel and provider has its own policy, so this is a channels by providers
// table, not one shared setting.
type Providers map[string][]Provider

// Provider is one provider on one channel and the policy that applies to it.
type Provider struct {
	Name     string        `yaml:"name"`     // identifies the provider in logs and in the notification row
	Priority int           `yaml:"priority"` // position in the fallback chain: 1 is tried first, then 2, and so on
	Kind     string        `yaml:"kind"`     // which adapter to use; empty means mock
	RPS      float64       `yaml:"rps"`      // outbound calls per second to this provider on this channel
	Timeout  time.Duration `yaml:"timeout"`  // limit for one call to it
	Breaker  Breaker       `yaml:"breaker"`

	// WebhookSecret is the key the provider signs its webhooks with. It is
	// optional: only cmd/webhook uses it (and checks its length), and a
	// provider without one cannot deliver webhooks. Being set through a
	// placeholder, it can only hold letters, digits and . _ + - (hex is a good
	// choice).
	WebhookSecret Secret `yaml:"webhook_secret"`
}

// Secret is a credential that prints as [redacted] with %v, %+v, %#v, in JSON and
// in slog, so logging a Provider or the whole chain cannot leak it. Reveal returns
// the value.
type Secret string

// Reveal returns the secret itself.
func (s Secret) Reveal() string { return string(s) }

// String implements fmt.Stringer.
func (s Secret) String() string { return redacted(s) }

// GoString implements fmt.GoStringer.
func (s Secret) GoString() string { return redacted(s) }

// LogValue implements slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(redacted(s)) }

// MarshalJSON implements json.Marshaler, which slog's JSON handler uses for a
// Secret nested in a struct.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redacted(s)) }

func redacted(s Secret) string {
	if s == "" {
		return ""
	}
	return "[redacted]"
}

// Breaker is the circuit breaker policy of one provider.
type Breaker struct {
	Failures int           `yaml:"failures"` // consecutive failures that open it
	OpenFor  time.Duration `yaml:"open_for"` // how long it stays open before one probe
}

// LoadProviders reads the providers file at path. See ParseProviders.
func LoadProviders(path string, getenv func(string) string) (Providers, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read providers file: %w", err)
	}
	p, err := ParseProviders(data, getenv)
	if err != nil {
		return nil, fmt.Errorf("providers file %s: %w", path, err)
	}
	return p, nil
}

// placeholder matches ${NAME} and ${NAME:-default}.
var placeholder = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// safeValue limits what an environment value may contain: numbers and
// durations such as 2s or 50.5. Anything else, a colon, brace, comma, hash or
// newline, could change the structure of the YAML it is substituted into.
var safeValue = regexp.MustCompile(`^[A-Za-z0-9._+-]+$`)

// minDuration is the shortest timeout accepted. A bare number in a duration
// field, like 2 for two seconds, would otherwise be read as 2 nanoseconds.
const minDuration = time.Millisecond

// ParseProviders parses the providers YAML. Values come from the environment
// through ${NAME} or ${NAME:-default} placeholders, which are replaced before
// the YAML is read, so they work anywhere a value does (and in comments, so
// keep them out of those). An unset or empty variable takes the default; with
// no default it is an error. A value may hold only letters, digits and . _ + -
// so it cannot alter the YAML around it. Unknown fields are rejected, and every
// invalid value is reported.
func ParseProviders(data []byte, getenv func(string) string) (Providers, error) {
	var errs []error
	expanded := placeholder.ReplaceAllFunc(data, func(m []byte) []byte {
		sub := placeholder.FindSubmatch(m)
		if v := getenv(string(sub[1])); v != "" {
			if !safeValue.MatchString(v) {
				errs = append(errs, fmt.Errorf("environment variable %s: %s has characters other than letters, digits and . _ + -", sub[1], shown(string(sub[1]), v)))
				return nil
			}
			return []byte(v)
		}
		if bytes.Contains(m, []byte(":-")) {
			if len(sub[2]) > 0 && !safeValue.Match(sub[2]) {
				errs = append(errs, fmt.Errorf("default of %s: %q has characters other than letters, digits and . _ + -", sub[1], sub[2]))
				return nil
			}
			return sub[2]
		}
		errs = append(errs, fmt.Errorf("environment variable %s is not set", sub[1]))
		return nil
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	var p Providers
	if err := dec.Decode(&p); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	for _, list := range p {
		sort.SliceStable(list, func(i, j int) bool { return list[i].Priority < list[j].Priority })
	}
	return p, nil
}

// shown quotes an environment value for an error message. A variable whose
// name says it holds a credential (SECRET, KEY, TOKEN or PASS in the name) is
// not echoed, since errors reach the logs. Other values are, so a bad rps or
// timeout can be diagnosed; name a credential variable accordingly.
func shown(name, value string) string {
	upper := strings.ToUpper(name)
	for _, w := range []string{"SECRET", "KEY", "TOKEN", "PASS"} {
		if strings.Contains(upper, w) {
			return "the value (not shown, the name says it is a credential)"
		}
	}
	return fmt.Sprintf("%q", value)
}

func (p Providers) validate() error {
	if len(p) == 0 {
		return errors.New("no channels configured")
	}
	channels := make([]string, 0, len(p))
	for c := range p {
		channels = append(channels, c)
	}
	sort.Strings(channels)

	var errs []error
	for _, c := range channels {
		list := p[c]
		if len(list) == 0 {
			errs = append(errs, fmt.Errorf("%s: at least one provider is required", c))
		}
		seen := map[string]bool{}
		priorities := map[int]string{}
		for i, pr := range list {
			at := fmt.Sprintf("%s[%d]", c, i)
			switch {
			case pr.Name == "":
				errs = append(errs, fmt.Errorf("%s: name is required", at))
			case seen[pr.Name]:
				errs = append(errs, fmt.Errorf("%s: provider %q is listed twice", at, pr.Name))
			}
			if pr.Name != "" {
				seen[pr.Name] = true
			}
			if pr.Priority < 1 {
				errs = append(errs, fmt.Errorf("%s: provider %q: priority must be at least 1, got %d", at, pr.Name, pr.Priority))
			} else if other, dup := priorities[pr.Priority]; dup {
				errs = append(errs, fmt.Errorf("%s: provider %q: priority %d is already used by %q", at, pr.Name, pr.Priority, other))
			} else {
				priorities[pr.Priority] = pr.Name
			}
			if pr.RPS <= 0 || math.IsNaN(pr.RPS) || math.IsInf(pr.RPS, 0) {
				errs = append(errs, fmt.Errorf("%s: rps must be a positive number", at))
			}
			if pr.Timeout < minDuration {
				errs = append(errs, fmt.Errorf("%s: timeout must be a duration of at least 1ms, with a unit such as 2s", at))
			}
			if pr.Breaker.Failures < 1 {
				errs = append(errs, fmt.Errorf("%s: breaker.failures must be at least 1", at))
			}
			if pr.Breaker.OpenFor < minDuration {
				errs = append(errs, fmt.Errorf("%s: breaker.open_for must be a duration of at least 1ms, with a unit such as 30s", at))
			}
		}
	}
	return errors.Join(errs...)
}
