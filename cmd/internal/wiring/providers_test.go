package wiring

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
	"github.com/mansonxasthur/eshaar/internal/platform/config"
	"github.com/mansonxasthur/eshaar/internal/ports"
)

func spec(name string, priority int) config.Provider {
	return config.Provider{Name: name, Priority: priority, RPS: 10, Timeout: 2 * time.Second, Breaker: config.Breaker{Failures: 3, OpenFor: time.Minute}}
}

func allChannels(t *testing.T, mutate func(config.Providers)) config.Providers {
	t.Helper()
	specs := config.Providers{}
	for _, c := range domain.Channels() {
		specs[string(c)] = []config.Provider{spec(string(c)+"-a", 1), spec(string(c)+"-b", 2)}
	}
	if mutate != nil {
		mutate(specs)
	}
	return specs
}

func TestBuildRoutesKeepsOrderTimeoutsAndSeparateInstances(t *testing.T) {
	specs := allChannels(t, func(s config.Providers) {
		s["sms"] = append(s["sms"], config.Provider{Name: "sms-c", Priority: 3, Kind: "mock", RPS: 1, Timeout: 300 * time.Millisecond, Breaker: config.Breaker{Failures: 1, OpenFor: time.Second}})
	})
	routes, err := BuildRoutes(specs)
	if err != nil {
		t.Fatal(err)
	}
	sms := routes[domain.ChannelSMS]
	if len(sms) != 3 || sms[0].Provider.Name() != "sms-a" || sms[2].Provider.Name() != "sms-c" || sms[2].Timeout != 300*time.Millisecond {
		t.Fatalf("sms chain = %+v", sms)
	}
	if routes[domain.ChannelEmail][0].Provider == sms[0].Provider {
		t.Error("channels share a provider instance")
	}
	for _, r := range sms {
		if _, ok := r.Provider.(ports.StatusChecker); !ok {
			t.Errorf("%s lost its status check behind the breaker and limiter", r.Provider.Name())
		}
	}
}

func TestBuildRoutesReportsEveryProblem(t *testing.T) {
	specs := allChannels(t, func(s config.Providers) {
		delete(s, "whatsapp")
		s["pigeon"] = []config.Provider{spec("p", 1)}
		bad := spec("x", 1)
		bad.Kind = "twilio"
		s["email"] = []config.Provider{bad}
	})
	_, err := BuildRoutes(specs)
	if err == nil {
		t.Fatal("bad providers accepted")
	}
	for _, want := range []string{`unknown channel "pigeon"`, `unknown kind "twilio"`, "channel whatsapp has no providers"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

// The file shipped in infra must load and build with no environment set.
func TestShippedProvidersFileLoads(t *testing.T) {
	specs, err := config.LoadProviders("../../../infra/providers.yaml", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildRoutes(specs); err != nil {
		t.Fatal(err)
	}
}

func TestBuildRoutesOrdersByPriority(t *testing.T) {
	specs := allChannels(t, func(s config.Providers) {
		s["sms"] = []config.Provider{spec("third", 30), spec("first", 1), spec("second", 2)}
	})
	routes, err := BuildRoutes(specs)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range routes[domain.ChannelSMS] {
		got = append(got, r.Provider.Name())
	}
	if strings.Join(got, ",") != "first,second,third" {
		t.Errorf("chain = %v, want priority order", got)
	}
}

func TestBuildRoutesAppliesMockBehavior(t *testing.T) {
	specs := allChannels(t, func(s config.Providers) {
		a := s["sms"][0]
		a.Mock = config.Mock{ErrorRate: 1}
		s["sms"][0] = a
	})
	routes, err := BuildRoutes(specs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := routes[domain.ChannelSMS][0].Provider.Send(context.Background(), domain.Notification{}); !errors.Is(err, ports.ErrTransient) {
		t.Errorf("sms-a with error_rate 1: err = %v", err)
	}
	if _, err := routes[domain.ChannelSMS][1].Provider.Send(context.Background(), domain.Notification{}); err != nil {
		t.Errorf("sms-b: %v", err)
	}
}
