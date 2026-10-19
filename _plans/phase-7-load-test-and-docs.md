# Phase 7: Load test, API docs and solution document — plan

## Context

Phases 1–6 are merged. Phase 7 (`docs/implementation_plan.md:111`) is the last one and is mostly measurement and writing: prove behaviour under load, show what happens when a provider fails, document the API, write the solution document from the Decision Log, and link the AI conversation. The reviewers read the code and do not run it, so results and reasoning must be written down in the repo.

Gaps found while exploring:
- Nothing in the repo can generate load or run the services as a stack: `infra/docker-compose.yml` has only Postgres and Kafka; the four processes run from `bin/` or `go run`.
- The config-built mock provider is `mock.NewProvider(spec.Name)` (`cmd/internal/wiring/providers.go:67`), with no latency or failure knobs. Those exist only as Go options (`WithDelay`, `WithError`, `WithRateLimited`, `WithScript`, `WithLateDelivery`...), so a provider-failure scenario cannot be driven from `providers.yaml` today. The mock also has no latency by default, so a load test would measure only our own overhead (acceptable, but must be stated).
- API surface to document: `POST /v1/notifications` (dispatcher, `internal/adapters/httpapi/handler.go:41`), `POST /v1/webhooks/{provider}` and `GET /healthz` on dispatcher and webhook (`httpapi/webhook.go:40`).

## Workflow (per CLAUDE.md)

1. `git switch main && git pull`, `git switch -c phase-7-load-test-and-docs`.
2. `gh issue create` "Phase 7: Load test, API docs and solution document" with the steps below as a checklist, before any code. Copy this plan to `_plans/phase-7-load-test-and-docs.md`.
3. One commit per step, each preceded by `/code-review` and `make verify`; `Refs #N`, last one `Closes #N`; no Co-Authored-By footer.
4. PR from the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.

## Steps

### 1. Mock behaviour from config (small code change)
- Add optional mock-only fields to a provider entry in `infra/providers.yaml` / `internal/platform/config/providers.go` (e.g. `mock: {latency: 50ms, error_rate: 0.0, timeout_rate: 0.0}`, each env-overridable like the other placeholders; defaults keep today's behaviour).
- `newProvider` in `cmd/internal/wiring/providers.go` translates them into mock options (`WithDelay`, `WithError`); add a seeded random mode to `internal/adapters/mock` (`WithFailureRate`) rather than a script. Unit tests in `mock` and `config`, and a wiring test.
- This is the only way to run the provider-failure scenario against real processes, and it keeps hexagonal rules (config → wiring in `cmd/internal`, mock stays an adapter).

### 2. Load-test tooling
- `loadtest/` (Go, under `cmd/loadtest` so `make build` and lint cover it; no new heavy dependency, plain `net/http` with a worker pool and a latency histogram). Flags: target URL, API key, rps or concurrency, duration, ramp steps, unique `idempotency_key` per request, mixed channels.
- Output: per-step achieved rps, p50/p95/p99, error and 429 counts, as a table and JSON.
- A second phase of the same tool (or a small SQL script) measures end-to-end delivery time: count `notifications` rows by state and `accepted_at - created_at` percentiles from Postgres, to check the "under 5 seconds" target.
- `make loadtest` target and `infra/` compose profile `stack` that runs migrate + dispatcher + sender + webhook + janitor from a small multi-stage Dockerfile (`infra/Dockerfile`), so a run is reproducible: `make up`, `make migrate`, start processes, `make loadtest`.
- The in-memory rate limit (`RATE_LIMIT_RPS`, default 1000/key) must be raised or several keys used for the test; record it.

### 3. Run and record the load test
- Scaled-down run on this machine: ramp the dispatcher alone (Kafka confirm path) to find its saturation point, then the full pipeline. Find which resource saturates first (CPU of each process, Kafka single node, Postgres write rate) using `docker stats`/`top`; vary `SENDER_POOL_SIZE`, `SENDER_BATCH_SIZE` and partition count between runs.
- Extrapolate: instances = 500K / per-instance rate for dispatcher and sender, and the Postgres write rate (~1M+ writes/s at peak, as the decision log notes) as the part that cannot be scaled by adding instances, with the proposal (partitioning/sharding, batch inserts).
- State plainly the limits: one machine, co-located load generator, mock with configurable latency, single-node Kafka/Postgres.
- Record in `docs/load_test.md`: hardware, versions, config, commands, raw tables, the saturation analysis and the extrapolation.

### 4. Provider failure scenario
- Using step 1 knobs, run with sms-a at 100% errors, then at timeouts (latency above its 2s timeout), then rate-limited, under steady load below saturation. Observe: fallback to sms-b, breaker opening after 5 failures and the 30s probe, latency impact, rows left `pending`/`unresolved`, janitor re-drives, duplicates (count provider-side sends per key where the status check is disabled, to show the documented trade-off).
- Also a "both providers down" case to show backpressure (pool full → consumer stops reading → Kafka lag) and recovery.
- Evidence from structured logs (attempt logs) and DB state counts. Record in `docs/load_test.md` under a "Provider failure" heading.

### 5. API documentation
- `docs/api.md` (the plan says the AI agent writes it at the end): request and response schema for `POST /v1/notifications` taken from `domain.Request` and the handler (status codes 202/400/401/413/429/503, validation rules, idempotency semantics), the webhook endpoint (signature header and HMAC scheme, event body, 200-after-store), `/healthz`, env/config reference, with curl examples checked against the running stack. Optionally an `openapi.yaml` derived from the same source; recommended only if cheap, since reviewers read rather than run.
- Verify every code/status claim against the handlers and tests, not memory.

### 6. Solution document
- `docs/solution.md`, built from `docs/decision_log.md`: problem and requirements, architecture diagram (components and message flow; Mermaid in markdown, one for the runtime flow and one for the state machine), main trade-offs and why, main difficulties and fixes (duplicate-on-timeout, save-before-send, hot partitions, janitor vs slow pool, early webhooks), load test and failure results (link `docs/load_test.md`), scaling to 500K/s (Kafka partitions, Postgres write rate, instances), PII note, ideas for a better solution with more time (outbox, sharded/partitioned tables or a different store, dead-letter topic, real provider adapters, distributed breakers/limiters, status-check by provider idempotency, observability stack). Covers every item of the "Testing and delivery checklist" at `implementation_plan.md:121`.
- Remove the "Reserved for the solution document" entries from the decision log as they get answered, and add a Load test section with decisions taken (e.g. mock knobs in config).

### 7. Conversation link and housekeeping
- Add the AI conversation link to `docs/solution.md` and the README-level entry point. **The link itself is the user's to supply** (the session sharing URL is not available to me); leave a clearly marked placeholder until they provide it.
- Write `docs/sessions/2026-10-19-phase-7-load-test-and-docs.md` like earlier phases, mark Phase 7 done in `docs/implementation_plan.md` and tick the checklist, update `CLAUDE.md` for `cmd/loadtest` and the new config fields.
- Optionally a short root `README.md` pointing to the solution doc, API doc, load test and how to run (does not exist yet).

## Critical files
- New: `cmd/loadtest/`, `infra/Dockerfile`, `docs/load_test.md`, `docs/api.md`, `docs/solution.md`, `docs/sessions/…phase-7…md`, `_plans/phase-7-…md`
- Changed: `infra/providers.yaml`, `infra/docker-compose.yml`, `Makefile`, `internal/platform/config/providers.go`, `cmd/internal/wiring/providers.go`, `internal/adapters/mock/provider.go`, `docs/decision_log.md`, `docs/implementation_plan.md`, `CLAUDE.md`
- Reused: `mock` options (`WithDelay`, `WithError`, `WithRateLimited`), `config.ParseProviders` env-placeholder mechanism, `httpapi.Handler`, existing `cmd/*/e2e_test.go` setup for starting testcontainers, `sender.WithBreaker`/`WithRateLimit`.

## Verification
- `make verify` before every commit (new tests for config fields, mock failure rate, wiring, and loadtest's histogram/step logic).
- Smoke: `make up && make migrate`, start the four processes, run `loadtest` for 10s at low rate, confirm 202s, rows reach `accepted`, and the DB-side latency query returns plausible numbers.
- Provider-failure runs reproduce from the commands written in `docs/load_test.md`; the doc's numbers are copied from actual output, not estimated.
- Docs: curl examples in `docs/api.md` executed against the stack; Mermaid diagrams render on GitHub; checklist at `implementation_plan.md:121` fully ticked except the conversation link until supplied.
