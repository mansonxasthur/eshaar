package main

import (
	"math"
	"sort"
	"time"
)

// result is the outcome of one request.
type result struct {
	latency time.Duration
	status  int // HTTP status, or 0 when the request failed before an answer
	done    time.Time
}

// stepReport summarizes one rate step.
type stepReport struct {
	TargetRPS   int     `json:"target_rps"`
	AchievedRPS float64 `json:"achieved_rps"` // accepted (202) per second
	Sent        int     `json:"sent"`
	Accepted    int     `json:"accepted"`
	RateLimited int     `json:"rate_limited"` // 429
	Unavailable int     `json:"unavailable"`  // 5xx
	Failed      int     `json:"failed"`       // no answer, or another status
	Behind      int     `json:"behind"`       // requests not started because every worker was busy
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
}

// summarize turns the results of one step into a report. elapsed is the window
// in which requests were offered and end its closing time: only answers that
// came in by end count towards the accepted rate. behind counts the requests
// the generator could not start.
func summarize(target int, results []result, behind int, elapsed time.Duration, end time.Time) stepReport {
	r := stepReport{TargetRPS: target, Sent: len(results), Behind: behind}
	lat := make([]time.Duration, 0, len(results))
	inWindow := 0
	for _, res := range results {
		switch {
		case res.status == 202:
			r.Accepted++
			if !res.done.After(end) {
				inWindow++
			}
		case res.status == 429:
			r.RateLimited++
		case res.status >= 500:
			r.Unavailable++
		default:
			r.Failed++
		}
		if res.status != 0 { // a request that got no answer has no latency worth ranking
			lat = append(lat, res.latency)
		}
	}
	if elapsed > 0 {
		r.AchievedRPS = float64(inWindow) / elapsed.Seconds()
	}
	r.P50Ms = percentile(lat, 0.50)
	r.P95Ms = percentile(lat, 0.95)
	r.P99Ms = percentile(lat, 0.99)
	return r
}

// percentile returns the p-th quantile (0 to 1) of d in milliseconds, using the
// nearest-rank method. It sorts d.
func percentile(d []time.Duration, p float64) float64 {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	rank := int(math.Ceil(p*float64(len(d)))) - 1
	rank = min(max(rank, 0), len(d)-1)
	return float64(d[rank]) / float64(time.Millisecond)
}
