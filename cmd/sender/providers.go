package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/mansonxasthur/eshaar/internal/adapters/mock"
	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/ports"
	"github.com/mansonxasthur/eshaar/internal/sender"
)

// buildRoutes turns the providers file into a fallback chain per channel. Every
// channel and provider gets its own adapter, circuit breaker and rate limiter,
// so one provider's outage or throttling on one channel does not touch another.
// Every supported channel must be configured.
func buildRoutes(specs config.Providers) (map[domain.Channel][]sender.Route, error) {
	var errs []error
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)

	routes := make(map[domain.Channel][]sender.Route, len(specs))
	for _, name := range names {
		channel, err := domain.ParseChannel(name)
		if err != nil {
			errs = append(errs, fmt.Errorf("providers: %w", err))
			continue
		}
		// Priority decides the order, whatever order the specs arrive in.
		chain := append([]config.Provider(nil), specs[name]...)
		sort.SliceStable(chain, func(i, j int) bool { return chain[i].Priority < chain[j].Priority })
		for _, spec := range chain {
			p, err := newProvider(spec)
			if err != nil {
				errs = append(errs, fmt.Errorf("providers: %s: %w", name, err))
				continue
			}
			routes[channel] = append(routes[channel], sender.Route{Provider: p, Timeout: spec.Timeout})
		}
	}
	for _, c := range domain.Channels() {
		if _, ok := specs[string(c)]; !ok {
			errs = append(errs, fmt.Errorf("providers: channel %s has no providers", c))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return routes, nil
}

// newProvider builds one provider from its spec: the adapter, inside the
// outbound rate limit and, outside that, a circuit breaker, so an open breaker
// fails fast without using a limiter token. The limiter's own wait errors never
// count against the breaker.
func newProvider(spec config.Provider) (ports.Provider, error) {
	var p ports.Provider
	switch spec.Kind {
	case "", "mock":
		p = mock.NewProvider(spec.Name)
	default:
		return nil, fmt.Errorf("provider %s: unknown kind %q", spec.Name, spec.Kind)
	}
	p = sender.WithRateLimit(p, spec.RPS)
	return sender.WithBreaker(p, sender.BreakerConfig{FailureThreshold: spec.Breaker.Failures, OpenFor: spec.Breaker.OpenFor}), nil
}
