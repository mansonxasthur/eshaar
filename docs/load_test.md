# Load test and provider failure results

All numbers below were measured on one machine with the commands in "How to reproduce". The target is 500K requests/s at peak with delivery under 5 seconds. One machine cannot reach that, so this test measures what one instance of each part can do, finds what saturates first, and extrapolates.

## Setup

| | |
| --- | --- |
| Machine | Intel Core i5-4570 (4 cores, 3.2 GHz), 15 GB RAM, Linux 7.0, Docker 26.1.3 |
| Everything on that one machine | the load generator, the dispatcher, sender, webhook and janitor (one container each), single-node Kafka 3.8.0 (12 partitions, replication 1) and Postgres 16 (default settings: `synchronous_commit=on`, `shared_buffers=128MB`) |
| Request | one notification with all three channels (email, SMS, WhatsApp), so **one request makes three rows** and three provider calls. Provider-failure runs use two channels (email, SMS) |
| Providers | the mock, answering at once and never failing unless a run says so |
| Stack settings | sender pool 256, batch 500, API key rate limit and provider rps raised so they do not limit the run (`RATE_LIMIT_RPS=1000000`, provider rps 100000) |
| Generator | `cmd/loadtest`, open loop: it offers a fixed rate and counts a request it could not start (all 512 workers busy) as "behind" |

What this setup does and does not show:

- The generator, Kafka and Postgres share four cores with the services, so every figure is a **lower bound** for a dedicated instance, and the dispatcher figure in particular.
- The mock answers instantly. Real providers add latency, which holds a pool slot longer and lowers what a pool of 256 can do; see the extrapolation.
- Delivery time is measured from the sender's insert to the provider's accept (`accepted_at - created_at`). It leaves out the dispatcher and the time a message waits in Kafka. When the sender falls behind, that wait is the part that grows, so the tables below also give how long the backlog took to clear after the generator stopped.

## 1. Where it saturates

### Dispatcher (request in, 202 out)

One dispatcher instance, 15-second steps, with the sender consuming at the same time:

| Target req/s | Accepted/s | p50 ms | p95 ms | p99 ms | Not started (behind) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1,000 | 0.8 | 12.0 | 25.2 | 0 |
| 2,000 | 2,000 | 1.0 | 9.0 | 22.5 | 0 |
| 4,000 | 4,000 | 2.2 | 13.3 | 29.6 | 0 |
| 8,000 | 7,999 | 4.4 | 36.4 | 79.7 | 0 |
| 12,000 | 11,447 | 11.8 | 67.9 | 98.1 | 8,208 |
| 16,000 | 14,783 | 29.1 | 60.0 | 78.7 | 18,228 |

No request failed or was refused at any step. Above about 8,000/s the target is no longer met and the generator starts to fall behind; the best rate seen was **about 14,800 accepted/s**. That is not a clean dispatcher ceiling: the generator, Kafka (125 to 170 % of a core in these runs) and the sender and Postgres compete for the same four cores. A dedicated dispatcher host would do more.

### Sender and Postgres (Kafka in, provider call, row accepted)

This is where the system saturates first, and far below the dispatcher. Steady 30-second runs at one rate, three rows per request:

| Req/s | Rows/s | Insert to accept p50 / p95 / p99 (ms) | Rows over 5 s | Backlog cleared after the run ends |
| ---: | ---: | ---: | ---: | ---: |
| 300 | 900 | 2.5 / 22 / 160 | 0 | not measured |
| 600 | 1,800 | 3.3 / 28 / 47 | 0 | not measured |
| 900 | 2,700 | 269 / 572 / 746 | 0 | 2 s |
| 1,200 | 3,600 | 378 / 668 / 827 | 0 | 13 s |

Up to 600 req/s (1,800 rows/s) the sender keeps up: insert-to-accept stays in milliseconds (the backlog was not timed in those two runs). At 900 it queues (median 270 ms), and at 1,200 it is clearly behind: the last message of the 30-second run was accepted 13 seconds after the run ended, so end-to-end delivery for those requests was well over the 5-second target even though the insert-to-accept column stays under one second. **The sustainable rate of one sender with default Postgres settings was about 900 req/s (2,700 rows/s).**

### What is the bottleneck

During a run at 1,200 req/s: Postgres 79 to 89 % CPU, sender 23 to 34 %, dispatcher 28 to 36 %. The sender is not the limit. In a 10-second window while the sender drained a backlog, Postgres logged 26,500 commits for 27,000 accepted rows and 25,800 row updates, with **no HOT updates**: `MarkAccepted` is one `UPDATE` per notification, each in its own transaction with its own WAL flush, and it changes indexed columns, so every update also writes index entries.

Two changes at 1,200 req/s confirm this:

| Change | Insert to accept p50 / p95 / p99 (ms) | Rows over 5 s | Backlog cleared after the run |
| ---: | ---: | ---: | ---: |
| none (reference, from the table above) | 378 / 668 / 827 | 0 | 13 s |
| `SENDER_POOL_SIZE` 256 to 1024 | 666 / 1,270 / 4,645 | 218 | 23 s |
| Postgres `synchronous_commit=off` | 5 / 78 / 116 | 0 | 1 s |

A larger pool does not help; it makes it worse because more calls wait on the same database. Taking the WAL flush out of the commit removes the queue. The limit is the cost of a durable commit per row.

With `synchronous_commit=off` the ramp continued (30-second runs, three rows per request):

| Req/s | Rows/s | Insert to accept p50 / p95 / p99 (ms) | Backlog cleared after the run |
| ---: | ---: | ---: | ---: |
| 2,500 | 7,500 | 182 / 408 / 521 | 16 s |
| 4,000 | 12,000 | 171 / 419 / 533 | 38 s |
| 6,000 | 18,000 | 164 / 426 / 553 | 65 s |

The sender then sustains about **5,500 rows/s (about 1,800 req/s)**, calculated as the rows of a run divided by the run plus the time it took to clear, with Postgres still at 73 to 86 % of the shared cores and the sender at 26 to 30 %. This setting trades a few hundred milliseconds of committed rows after a Postgres crash for speed, which breaks save-before-send, so it is evidence and not a recommendation.

## 2. Extrapolation to 500K req/s

Instances needed = 500,000 divided by the per-instance rate. The inputs are measured here, and the result is an estimate, not a test result.

| Part | Measured per instance | Instances for 500K req/s |
| --- | --- | --- |
| Dispatcher | at least 14,800 req/s (shared machine; lower bound) | **about 34 or fewer**, behind a load balancer, with a Kafka producer that batches and compresses |
| Sender | CPU is not the limit: about 0.3 of a core at 5,500 rows/s, so roughly 18,000 rows/s per core; at 3 rows per request and 4 cores that is about 24,000 req/s | about 21 at 4 cores each, **if the database could take the writes**, and 12 partitions are too few: the consumer group needs at least one partition per instance, so use 64 or more |
| Postgres | 2,700 rows/s (900 req/s) with a commit per row; 5,500 rows/s without the flush | see below |

The part that does not scale by adding instances is Postgres. At 500K req/s with three rows per request the peak is 1.5M inserted rows/s and 1.5M row updates/s, 3M writes/s, which is what the decision log already estimated. At the measured 2,700 rows/s per node (one insert and one update per row, each update its own commit) that is more than 550 such nodes. Three changes would close most of that gap, in this order:

1. **Batch the outcome writes.** Insert is already one statement per batch of up to 500 rows (`InsertPending`); `MarkAccepted` is not. Writing the outcomes of a batch in one `UPDATE ... FROM unnest(...)` removes the per-row commit, which the `synchronous_commit` run suggests is worth about 2x on this machine and more with larger batches. This is the highest-value change and is listed as follow-up work, not done in this phase.
2. **Partition or shard `notifications`** by idempotency key hash across several Postgres primaries, so each takes a share of the writes. Insert, outcome and janitor queries all carry the key or the row id.
3. **Fewer indexes on the hot path.** The updates are not HOT because they touch indexed columns (`state`, `provider_message_id`, `last_provider`); keeping the unique and partial indexes the design needs but fillfactor-tuning and trimming the rest would let more updates stay in the page.

With those, the sender and database tier, not the dispatcher, set the cost of reaching 500K req/s.

## 3. When a provider fails

Procedure: `infra/provider-failure.sh`. It empties the table, restarts the sender and janitor with the setting (which also resets the circuit breakers), offers 600 req/s of email and SMS for 30 seconds (about 18,000 requests, 36,000 rows), and waits for pending rows to settle. The SMS provider `sms-a` is the one that fails; its fallback is `sms-b`. Email is unaffected in every run, with 17,999 of 17,999 rows accepted by `email-a`.

| Scenario | `sms-a` calls made | Calls rejected by its open breaker | SMS accepted by `sms-b` | Insert to accept for SMS, p50 / p99 / max (ms) | Rows not settled |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fails every call (`SMS_A_ERROR_RATE=1`) | 5 | 17,994 | 17,999 | 2.9 / 45 / 185 | 0 |
| Fails half its calls (`SMS_A_ERROR_RATE=0.5`) | 24 | 17,950 | 17,974 (25 stayed on `sms-a`) | 2.5 / 20 / 84 | 0 |
| Slower than its timeout (`SMS_A_LATENCY=3s`, timeout 2 s) | 258 (all timeouts) | 17,741 | 17,999 | 3.1 / 2,055 / 3,602 | 0 |
| Rate limited to 100 rps (`SMS_A_RPS=100`) | 3,009 accepted, 290 timeouts | 0 (14,700 limiter waits ended in a failed attempt) | 14,990 | 3.1 / 2,022 / 2,168 | 0 |
| Both SMS providers fail (`SMS_A_ERROR_RATE=1 SMS_B_ERROR_RATE=1`) | 5 (each) | 17,994 and 17,993 | 0 | none accepted | 17,988 pending, 11 failed |

What each shows:

- **Hard failure:** the breaker opens after the configured 5 consecutive failures and the remaining requests skip the provider without a call. Nothing is lost, there is no latency cost (p99 45 ms), and nothing is re-driven. The 30-second `open_for` is longer than the run, so the half-open probe is not exercised here (it is covered by the breaker's unit tests).
- **Partial failure:** the failures are random, so consecutive failures are rare enough that the breaker opens later (after 24 real failures) and is then open for the rest of the run, which sends even the 50 % that would have worked to `sms-b`. A breaker on consecutive failures reacts late and then over-reacts to a flaky provider; a failure-rate window would be the alternative.
- **Timeouts:** each call that reaches a slow provider holds its pool slot for the full 2 seconds, and every one of the 258 timed-out requests waited out that time before falling back, which is the p99 of 2 seconds. Once the breaker opened (after 5 consecutive timeouts, with the calls already in flight finishing) the cost disappeared. This run is the one that can create duplicates: the mock does not take a message whose call times out, so the status check correctly finds nothing and the fallback sends once. A real provider that took the message and lost the reply is the documented duplicate case. A timeout followed by a successful fallback ends `accepted`; a row is `unresolved` only when a timeout (or a failed status check) is not followed by any provider accepting it, which none of these runs produced.
- **Rate limited:** the outbound limiter queues instead of rejecting, and the wait counts against the 2-second send timeout. Under a sustained overload every overflow request waits the whole timeout and then falls back, so p95 is 2.0 seconds for SMS while the first 3,009 are accepted by `sms-a`. The wait does not open the breaker, by design. A shorter wait bound would trade smoothing of bursts for lower latency under overload.
- **Both down:** 17,988 SMS rows stay `pending` because both breakers are open and no provider was tried (the sender logs "no outcome to record; row stays pending for the janitor" 19,688 times, once per attempt that ended this way). 11 rows are `failed`, from the calls made before the breakers opened. The janitor re-drove 1,700 of them in the following 150 seconds, each re-drive using one of the row's three attempts. Email is unaffected, and the dispatcher kept answering 202 at the same rate (p99 6.5 ms) because the backlog waits in Postgres, not in the request path.

### Recovery after the outage

The providers were restored (sender and janitor restarted without the setting) and the rows counted every 10 seconds:

| After restore | SMS accepted | SMS pending | Rows re-driven in total |
| ---: | ---: | ---: | ---: |
| 0 s | 50 | 17,937 | 1,900 |
| 60 s | 650 | 17,337 | 2,500 |
| 110 s | 1,150 | 16,837 | 3,000 |

Recovery is steady at **10 rows per second**: a janitor tick takes 50 rows (`JANITOR_BATCH_SIZE`) every 5 seconds (`JANITOR_INTERVAL`). Draining the 17,988 rows would take about 30 minutes, and a row that spent its 3 attempts during the outage (each re-drive that finds both providers unavailable uses one) goes to `unresolved` at the next sweeper run (every 10 minutes), even though the providers are healthy again; a late webhook can still move it to a final state. The defaults are tuned for gentle recovery of rare stalls, not for the end of a long provider outage. `JANITOR_BATCH_SIZE`, `JANITOR_CONCURRENCY` (which must not be below the batch size) and the number of janitor instances raise the rate; the attempt limit is a code constant. A real outage of this size needs either a bigger recovery rate or an operator-triggered re-drive; this is listed as a limit in the solution document.

## How to reproduce

```
make stack-up                    # Postgres, Kafka and the five processes (build takes a minute)
make loadtest LOADTEST_ARGS="-rates 1000,2000,4000,8000 -step 15s -workers 512"
make loadtest LOADTEST_ARGS="-rates 900 -step 30s"      # prints a run id
make loadtest-report RUN=<run id>                       # rows by state, insert-to-accept times, janitor activity
SMS_A_ERROR_RATE=1 infra/provider-failure.sh            # one failure scenario; see the script header for the others
SENDER_POOL_SIZE=1024 docker compose -f infra/docker-compose.yml --profile stack up -d sender
make stack-down COMPOSE_DOWN_FLAGS=-v
```

The `synchronous_commit` runs used `ALTER SYSTEM SET synchronous_commit = off` followed by `SELECT pg_reload_conf()`, and `ALTER SYSTEM RESET` afterwards. Between runs the table was emptied or the stack recreated with `-v`, so no run started with a backlog. Times and rates vary by a few percent from run to run on a shared machine.
