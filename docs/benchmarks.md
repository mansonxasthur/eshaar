# Benchmarks

Go micro-benchmarks for the hot paths. They complement the end-to-end load test (`docs/load_test.md`): the load test says how much the whole pipeline sustains, and these say what each piece costs, so a pull request can show a regression and the Postgres write cost can be compared before and after a change.

```
make bench-short                 # everything that needs no Docker
make bench                       # also the Postgres benchmarks (Docker)
make bench BENCHTIME=5s          # longer runs for steadier numbers
go test -run '^$' -bench Validate -benchmem ./internal/domain     # one benchmark
```

Compare two runs with `benchstat` (`go install golang.org/x/perf/cmd/benchstat@latest`): run each side with `-count=10`, save the outputs, and pass both files to it.

The Postgres benchmarks share one container for the whole run, which the testcontainers reaper removes when the process exits. This document links to `docs/load_test.md` and `docs/solution.md`, which arrive with the Phase 7 pull request.

## Baseline

Intel Core i5-4570 (4 cores), Linux, Go 1.26, Postgres 16-alpine in a container with default settings (durable commits). Single runs on a desktop that was also running other work, and a repeat run of the same benchmark differed by as much as 40 % (`Verify` measured 2.2 to 3.8 microseconds), so treat these as orders of magnitude and compare changes with `benchstat` over `-count=10`. `-4` means four CPUs; the "Parallel" benchmarks use all of them.

### Dispatcher

| Benchmark | ns/op | B/op | allocs/op | What it is |
| --- | ---: | ---: | ---: | --- |
| `BenchmarkRequestValidate` (`internal/domain`) | 26,039 | 4,824 | 62 | Validating a request with three channels |
| `BenchmarkRequestNotifications` (`internal/domain`) | 1,134 | 864 | 5 | Splitting it into rows (sender) |
| `BenchmarkCreateNotification` (`internal/adapters/httpapi`) | 35,894 | 10,772 | 73 | The whole handler: key check, rate limit, decode, validate, publish to a no-op publisher, answer |
| `BenchmarkCreateNotificationParallel` | 5,406 | 10,772 | 73 | The same on 4 CPUs (about 185,000 requests/s) |
| `BenchmarkAPIKeyAuth` | 1,813 | 576 | 7 | The key check alone, four configured keys (includes the recorder the harness allocates) |
| `BenchmarkEncode` / `BenchmarkDecode` (`internal/adapters/kafka`) | 4,424 / 9,632 | 624 / 1,064 | 8 / 21 | The Kafka record, written by the dispatcher and read by the sender |

### Sender

| Benchmark | ns/op | B/op | allocs/op | What it is |
| --- | ---: | ---: | ---: | --- |
| `BenchmarkDeliverFirstProvider` (`internal/sender`) | 6,918 | 1,584 | 25 | One channel send when the first provider accepts, with a fake store |
| `BenchmarkDeliverFallback` | 9,703 | 2,212 | 35 | The first provider fails and the second accepts |
| `BenchmarkDeliverParallel` | 1,039 | 1,704 | 27 | The first-provider path on 4 CPUs, through the real breaker and limiter |
| `BenchmarkBreakerClosedParallel` | 287 | 0 | 0 | The breaker around a healthy provider |
| `BenchmarkBreakerOpenParallel` | 147 | 0 | 0 | An open breaker rejecting a call (the outage case) |
| `BenchmarkRateLimitParallel` | 293 | 0 | 0 | The outbound limiter when it does not have to wait |

### Webhook

| Benchmark | ns/op | B/op | allocs/op | What it is |
| --- | ---: | ---: | ---: | --- |
| `BenchmarkSign` (`internal/webhook`) | 4,160 | 656 | 9 | The HMAC a provider computes (the work Verify does) |
| `BenchmarkVerify` | 3,815 | 560 | 8 | The HMAC signature check |
| `BenchmarkParseEvent` | 13,577 | 2,448 | 35 | Parsing one event and checking Postgres can store it |
| `BenchmarkReceiveParallel` | 7,485 | 4,532 | 60 | The service from verify to the flush, with production batch settings, about 1,000 requests waiting, and a store that answers at once. ns/op is the cost per event at throughput (about 130,000 events/s on this machine), not one request's latency |
| `BenchmarkWebhookReceiveHandler` (`internal/adapters/httpapi`) | 11,768 | 7,474 | 32 | The HTTP handler with a receiver that accepts at once. Includes the `httptest` request and recorder built in the loop, which is most of the bytes |

### Postgres

| Benchmark | Rows/s | ns/op | What it is |
| --- | ---: | ---: | --- |
| `BenchmarkInsertPending` | 24,000 to 26,000 | 5.7 to 6.3 million per batch | One polled batch: 50 requests with three channels, 150 rows, one statement |
| `BenchmarkMarkAccepted` | 3,700 to 4,600 | about 220,000 to 270,000 | The outcome write as the store does it: one `UPDATE` and one commit per row, one goroutine |
| `BenchmarkMarkAcceptedParallel` | 9,500 to 11,500 | about 87,000 to 106,000 | The same from 128 goroutines (the sender's pool calls it concurrently) |
| `BenchmarkMarkAcceptedBatchedReference` | 33,000 to 40,000 | about 25,000 to 30,000 | **Not in the store.** The same update for 100 rows in one `UPDATE ... FROM unnest(...)` |

## Reading the numbers

- **The dispatcher's handler is not what limits it.** The handler does about 185,000 requests/s on four CPUs against a no-op publisher, while the load test sustained about 14,800/s end to end. The difference is the HTTP server, the network, the Kafka produce and the other processes sharing the machine. Inside the handler, `Validate` is about 70 % of the time (26 of 36 microseconds), so it is the first place to look if the handler itself ever matters. It decodes each channel's block into a struct to check it.
- **Sender CPU is small.** A channel send costs 7 to 10 microseconds, and the breaker and limiter cost a few hundred nanoseconds and no allocation, so none of them is a concern next to a provider call that takes milliseconds. A fallback adds about 2.8 microseconds.
- **Postgres is.** Writing one outcome per commit gets 3,700 to 4,600 rows/s from one goroutine and 9,500 to 11,500 rows/s from 128 (concurrent commits share a WAL flush), against 33,000 to 40,000 rows/s when 100 outcomes go in one statement: **roughly 3 to 4 times the parallel per-row rate** (about 3.5 at the mid-points). (The Postgres figures are ranges over four repeated runs of the three outcome benchmarks.) This is the measured case for the first follow-up in `docs/solution.md` (batch the outcome writes). The benchmark's rates are above the load test's (2,700 rows/s per sender), because here Postgres has the machine to itself; what carries over is the ratio.
- **Webhooks batch well.** The service handles an event in about 7.5 microseconds at throughput, so a webhook process is limited by Postgres, not by its own CPU.

## What they do not cover

- The network, TLS and the Kafka produce and consume, which need a broker (see the load test).
- The janitor's claim queries, which have an `EXPLAIN` test but no timing benchmark.
- Provider latency: the mock answers at once, so the sender benchmarks show overhead only.
- Anything under a shared machine's contention, which is what the load test shows.
