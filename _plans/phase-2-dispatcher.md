# Phase 2: Dispatcher — plan

## Context

Phase 1 (domain, config, migrations, docker-compose) is merged on `origin/main` (PR #2). The local branch `ci-github-flow` is stale, so work starts from a fresh `origin/main`. Phase 2 delivers the dispatcher: an HTTP endpoint that authenticates by API key, rate-limits, validates the request, publishes to Kafka keyed by `idempotency_key`, and returns `202` only after Kafka confirms (`docs/implementation_plan.md`, Phase 2). Decisions made with the user: **franz-go** as the Kafka client, rate limiter **per API key**.

## Workflow (per CLAUDE.md)

1. `git fetch && git switch -c phase-2-dispatcher origin/main`.
2. `gh issue create` "Phase 2: Dispatcher" with a checklist (the steps below) before any code.
3. One commit per step, each preceded by `/code-review` and `make verify`; commits say `Refs #N`, the final one `Closes #N`. No Co-Authored-By footer.
4. Open a PR with the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.
5. Write `docs/sessions/2026-10-19-phase-2-dispatcher.md` (Phase 1 did the same) and update `docs/decision_log.md`.

## Design

**Layout (import rules from CLAUDE.md hold):**

- `internal/domain/request.go` — `Request` (idempotency_key, recipient_id, notification_type, email, phone, `Content map[Channel]json.RawMessage`) and `Validate()`. Domain-only, no JSON/HTTP. Rules: the three required fields non-empty; at least one channel in content; every content key is a known channel (`ParseChannel`); `email` required if an email block is present; `phone` required if sms or whatsapp is present; each block is non-empty valid JSON object (sms needs `text`; email/whatsapp need `template_id`). Returns every violation at once (`errors.Join`-style `ValidationError` with field-level messages) so the 400 body is clear.
- `internal/ports/publisher.go` — `Publisher` interface: `Publish(ctx, domain.Request) error` (returns only after the broker confirms). No Kafka types.
- `internal/adapters/kafka/publisher.go` — franz-go `kgo.Client`: `RequiredAcks(AllISRAcks)`, idempotent producer, `ProduceSync` of one record: key = idempotency_key, value = JSON of the request (UTF-8, Arabic unescaped via `json.Encoder.SetEscapeHTML(false)` and no ASCII escaping — Go's encoder keeps UTF-8). Batching/linger and compression configured via options for throughput. `Close()` for shutdown.
- `internal/adapters/http/` (package name `httpapi` to avoid clashing with `net/http`):
  - `middleware.go`: `APIKeyAuth(keys)` — reads `X-API-Key`, constant-time compare against the configured set, 401 on miss; `RateLimit(rps, burst)` — `golang.org/x/time/rate` limiter per API key in a mutex-guarded map (runs after auth, so unauthenticated traffic cannot grow the map), 429 + `Retry-After`.
  - `handler.go`: `POST /v1/notifications` — body size cap (`http.MaxBytesReader`), strict JSON decode (unknown fields rejected), `Validate()` → 400 with field errors, `Publisher.Publish` with a request-scoped timeout → `202 {"status":"accepted","idempotency_key":...}`; publish failure → `503` with a generic error body (details only in the log); `GET /healthz`.
  - `server.go`: builds the mux and `http.Server` with read/write/idle timeouts; graceful shutdown.
- `internal/platform/config`: add `DISPATCHER_API_KEYS` (comma-separated, required for the dispatcher), `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST` (defaults e.g. 1000/2000), new `NeedAPIKeys`-style flag following the existing `Need` pattern; validate and report with the existing joined-error approach.
- `cmd/dispatcher/main.go`: load config (`NeedKafka|NeedHTTP|…`), build logger once and pass down, construct the kafka publisher, the middleware chain and server, run until SIGINT/SIGTERM, then `Shutdown` and close the producer.

**Behavior notes:** `Publisher.Publish` failing or timing out never yields 202. A duplicate idempotency key is still published (dedup is the sender's unique constraint, per the decision log). Logs via the injected `slog` logger, never logging API keys or contact details.

## Steps / commits

1. Domain `Request` + validation + table tests (missing phone, missing email, unknown channel, empty content, bad block, Arabic text, multiple errors).
2. `Publisher` port + Kafka adapter + its test (below).
3. HTTP middleware (auth, rate limit) + tests.
4. Handler, server, config additions, `cmd/dispatcher` wiring + handler tests with a fake publisher.
5. End-to-end test, decision log + session log updates, `CLAUDE.md` note if commands change; `Closes #N`.

## Tests

- Unit: validation rules; key check (missing, wrong, right, constant-time path); rate limiter (burst then 429, independent keys, refill with an injected clock or small rates); handler (202 on success, 400 with field errors, 401, 429, 503 on publish error, 413 oversized body, malformed JSON); config.
- Kafka adapter + e2e: testcontainers Kafka (`modules/kafka`, KRaft) like the Phase 1 migration test — skipped under `-short` or without Docker. E2E starts the real handler chain with the real publisher, POSTs a request, and consumes the topic with a franz-go consumer to assert exactly one message, key == idempotency_key, and the value round-trips including the Arabic content byte-for-byte. A second e2e case with the broker unreachable/closed asserts `503` and no `202`.

## Critical files

New: `internal/domain/request.go`, `internal/ports/publisher.go`, `internal/adapters/kafka/`, `internal/adapters/http/`. Modified: `cmd/dispatcher/main.go`, `internal/platform/config/config.go`, `go.mod`/`go.sum` (franz-go, x/time, testcontainers kafka module), `docs/decision_log.md`. Reuse: `domain.Channels`/`ParseChannel`, `config.Load` + `Need` flags, `logging.New`, the testcontainers skip pattern in `internal/adapters/postgres/migrate_test.go`.

## Verification

- `make verify` green on every commit.
- Manual: `make up`, set `KAFKA_BROKERS=localhost:9092 DISPATCHER_API_KEYS=dev`, run `bin/dispatcher`, `curl -X POST :8080/v1/notifications -H 'X-API-Key: dev' -d @sample.json` (the decision log's Arabic sample) → 202; confirm the record with `kafka-console-consumer` (key = idempotency key, Arabic intact); wrong key → 401; omit phone with an sms block → 400; stop Kafka → 503.
- CI `verify` check green on the PR.
