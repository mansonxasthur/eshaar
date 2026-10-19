// Command loadtest drives POST /v1/notifications at a series of target rates
// and reports, per step, the accepted rate and the latency percentiles. It
// stops at the dispatcher's 202; what the sender does afterwards is read from
// Postgres with `make loadtest-report`.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	url := fs.String("url", "http://localhost:8080/v1/notifications", "dispatcher endpoint")
	keys := fs.String("keys", os.Getenv("LOADTEST_API_KEYS"), "comma-separated API keys, used round robin (one key is rate limited on its own)")
	rates := fs.String("rates", "200,500,1000", "comma-separated target requests per second, one step each")
	stepFor := fs.Duration("step", 10*time.Second, "how long each step lasts")
	workers := fs.Int("workers", 256, "most requests in flight")
	channels := fs.String("channels", "email,sms,whatsapp", "channels each request carries")
	asJSON := fs.Bool("json", false, "print the reports as JSON instead of a table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workers < 1 || *stepFor <= 0 {
		return fmt.Errorf("-workers must be at least 1 and -step positive")
	}
	keyList := splitList(*keys)
	if len(keyList) == 0 {
		return fmt.Errorf("no API keys: set -keys or LOADTEST_API_KEYS")
	}
	var targets []int
	for _, s := range splitList(*rates) {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return fmt.Errorf("bad rate %q", s)
		}
		targets = append(targets, n)
	}
	if len(targets) == 0 {
		return fmt.Errorf("no rates")
	}
	var chans []string
	for _, c := range splitList(*channels) {
		chans = append(chans, strings.ToLower(c))
	}
	if len(chans) == 0 {
		return fmt.Errorf("no channels")
	}
	if _, err := body("check", 0, chans); err != nil {
		return err
	}

	var runID [4]byte
	if _, err := rand.Read(runID[:]); err != nil {
		return err
	}
	g := &generator{
		url: *url, keys: keyList, channels: chans, run: hex.EncodeToString(runID[:]),
		workers: *workers,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: *workers, MaxConnsPerHost: *workers},
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var reports []stepReport
	for _, rate := range targets {
		if ctx.Err() != nil {
			break
		}
		reports = append(reports, g.step(ctx, rate, *stepFor))
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{"run": g.run, "steps": reports})
	}
	ew := &errWriter{w: out}
	ew.printf("run %s (idempotency keys start with %s-)\n", g.run, g.run)
	ew.printf("%8s %10s %9s %8s %6s %6s %7s %8s %8s %8s\n",
		"target", "accepted/s", "sent", "429", "5xx", "other", "behind", "p50 ms", "p95 ms", "p99 ms")
	for _, r := range reports {
		ew.printf("%8d %10.0f %9d %8d %6d %6d %7d %8.1f %8.1f %8.1f\n",
			r.TargetRPS, r.AchievedRPS, r.Sent, r.RateLimited, r.Unavailable, r.Failed, r.Behind, r.P50Ms, r.P95Ms, r.P99Ms)
	}
	return ew.err
}

// errWriter keeps the first write error, so the report code checks it once.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err == nil {
		_, e.err = fmt.Fprintf(e.w, format, args...)
	}
}

type generator struct {
	url      string
	keys     []string
	channels []string
	run      string
	workers  int
	client   *http.Client
	seq      atomic.Int64
}

// step offers rate requests per second for d. Work is handed to a fixed pool
// through a small buffer; when the buffer is full the request is counted as
// behind instead of queueing, so a saturated target shows up as a gap between
// the target and the accepted rate, not as latency that hides in a backlog.
func (g *generator) step(ctx context.Context, rate int, d time.Duration) stepReport {
	jobs := make(chan int64, g.workers)
	var (
		mu      sync.Mutex
		results []result
		wg      sync.WaitGroup
	)
	for range g.workers {
		wg.Go(func() {
			local := make([]result, 0, 1024)
			for seq := range jobs {
				if res, ok := g.one(ctx, seq); ok {
					local = append(local, res)
				}
			}
			mu.Lock()
			results = append(results, local...)
			mu.Unlock()
		})
	}

	start := time.Now()
	behind := 0
	const tick = time.Millisecond
	ticker := time.NewTicker(tick)
	last := start
	var owed float64
loop:
	for now := range ticker.C {
		if now.Sub(start) >= d || ctx.Err() != nil {
			break loop
		}
		// A ticker drops ticks when the process is busy, so the debt is
		// counted from the clock, not from the number of ticks.
		owed += float64(rate) * now.Sub(last).Seconds()
		last = now
		for ; owed >= 1; owed-- {
			select {
			case jobs <- g.seq.Add(1):
			default:
				behind++
			}
		}
	}
	ticker.Stop()
	// The rate is over the window in which requests were offered; waiting for
	// the last answers must not count against it.
	window := time.Since(start)
	end := start.Add(window)
	close(jobs)
	wg.Wait()
	return summarize(rate, results, behind, window, end)
}

// one sends request seq. ok is false when there is no sample to count: the
// request could not be built, or the run was interrupted while it was in flight.
func (g *generator) one(ctx context.Context, seq int64) (res result, ok bool) {
	b, err := body(g.run, seq, g.channels)
	if err != nil {
		return result{}, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.url, strings.NewReader(string(b)))
	if err != nil {
		return result{}, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", g.keys[int(seq)%len(g.keys)])
	t := time.Now()
	resp, err := g.client.Do(req)
	if err != nil {
		return result{latency: time.Since(t), done: time.Now()}, ctx.Err() == nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return result{latency: time.Since(t), status: resp.StatusCode, done: time.Now()}, true
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
