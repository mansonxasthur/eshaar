package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mansonxasthur/eshaar/internal/domain"
)

func TestPercentileUsesNearestRank(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[float64]float64{0.50: 50, 0.95: 95, 0.99: 99, 1: 100} {
		if got := percentile(d, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if percentile(nil, 0.5) != 0 {
		t.Error("empty input must give 0")
	}
}

func TestSummarizeCountsEachStatus(t *testing.T) {
	ms := time.Millisecond
	end := time.Now()
	early, late := end.Add(-ms), end.Add(ms)
	rs := []result{{ms, 202, early}, {ms, 202, early}, {ms, 202, late}, {ms, 429, early}, {ms, 503, early}, {ms, 0, early}, {ms, 400, early}}
	r := summarize(100, rs, 7, 2*time.Second, end)
	if r.Accepted != 3 || r.RateLimited != 1 || r.Unavailable != 1 || r.Failed != 2 || r.Behind != 7 || r.Sent != 7 {
		t.Errorf("report = %+v", r)
	}
	if r.P99Ms != 1 { // the request with no answer is not a latency sample
		t.Errorf("p99 = %v", r.P99Ms)
	}
	if r.AchievedRPS != 1 { // the answer that came after the window is not in the rate
		t.Errorf("achieved = %v, want 1", r.AchievedRPS)
	}
}

func TestBodyIsAValidRequestWithUniqueKeys(t *testing.T) {
	seen := map[string]bool{}
	for seq := range int64(3) {
		b, err := body("run", seq, []string{"email", "sms", "whatsapp"})
		if err != nil {
			t.Fatal(err)
		}
		var req domain.Request
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatal(err)
		}
		if err := req.Validate(); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		if seen[req.IdempotencyKey] {
			t.Errorf("duplicate key %s", req.IdempotencyKey)
		}
		seen[req.IdempotencyKey] = true
	}
	if _, err := body("run", 0, []string{"fax"}); err == nil {
		t.Error("unknown channel accepted")
	}
}

func TestRunDrivesTheEndpointAndReports(t *testing.T) {
	var hits atomic.Int64
	var wrongKey atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-API-Key") == "" {
			wrongKey.Store(true)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	var out bytes.Buffer
	args := []string{"-url", srv.URL, "-keys", "k1,k2", "-rates", "200", "-step", "300ms", "-workers", "8", "-json"}
	if err := run(args, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Steps []stepReport `json:"steps"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if len(got.Steps) != 1 || got.Steps[0].Accepted == 0 || int64(got.Steps[0].Sent) != hits.Load() {
		t.Errorf("steps = %+v, hits = %d", got.Steps, hits.Load())
	}
	if wrongKey.Load() {
		t.Error("a request had no API key")
	}
}

func TestRunRejectsBadFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-keys", ""},
		{"-keys", "k", "-rates", "x"},
		{"-keys", "k", "-rates", ","},
		{"-keys", "k", "-channels", "fax"},
		{"-keys", "k", "-channels", ","},
		{"-keys", "k", "-workers", "0"},
		{"-keys", "k", "-step", "0s"},
	} {
		if err := run(args, &bytes.Buffer{}); err == nil || strings.TrimSpace(err.Error()) == "" {
			t.Errorf("run(%v) accepted bad input", args)
		}
	}
}
