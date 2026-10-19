# Phase 1: Foundations

## Context
Phase 1 of `docs/implementation_plan.md` delivers a runnable skeleton (Kafka, Postgres, schema) before any business logic. Already done: hexagonal layout (doc-only packages), four `cmd/*` stubs, `internal/platform/logging`. Missing: config loading, local env, migrations, index decisions, domain model + state machine, and the two required tests (state machine unit tests; migration test for both unique constraints).

## Steps

0. **GitHub issue first** (CLAUDE.md): `gh issue create` "Phase 1: Foundations" with a checklist of steps 1-6. Reference it in every commit (`Refs #N`), `Closes #N` on the last. Run `/code-review` + `make verify` before each commit; no Co-Authored-By.

1. **Domain** (`internal/domain`, stdlib only)
   - `channel.go`: `Channel` (email, sms, whatsapp), `ParseChannel`.
   - `state.go`: `State` (pending, accepted, delivered, failed, unresolved); `CanTransition(from, to)` table, forward-only. Allowed: pending→accepted, pending→unresolved, accepted→delivered|failed, unresolved→delivered|failed. Delivered/failed are terminal. Open points decided and logged: pending→delivered/failed rejected (webhook arriving before accept is resolved by the webhook adapter after the accept is saved); accepted→unresolved allowed (sweeper gives up on stale claimed rows) — confirm against decision log wording when writing.
   - `notification.go`: `Notification` struct mirroring the `notifications` columns (one per channel), per-channel `Content` as `json.RawMessage`, `Transition(to, now)` returning `ErrInvalidTransition`.
   - `webhook_event.go`: `WebhookEvent` struct.
   - Tests `state_test.go`: table test covering every (from,to) pair, explicitly the named cases (pending→accepted, accepted→final, pending→unresolved, unresolved→final, all backward moves rejected, final states immutable).

2. **Config** (`internal/platform/config`)
   - Env-based struct: `DATABASE_URL`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, listen addrs, plus existing `LOG_LEVEL`. `Load(getenv func(string) string)` for testability, validation errors listed together. Unit test with a fake getenv. Wire into `cmd/*` mains (load, log, exit non-zero on error); no business logic yet.

3. **Migrations** (`internal/adapters/postgres/migrations/*.sql`, embedded via `embed` + golang-migrate `iofs`)
   - `0001_notifications`: columns per decision log; `UNIQUE (idempotency_key, channel)`; CHECK on `channel` and `state`; `content jsonb`.
   - `0002_webhook_events`: columns per decision log; `UNIQUE (provider, event_id)`; `notification_id` FK.
   - **Indexes (decided here, recorded in decision log):**
     - Partial `notifications (created_at) WHERE state='pending' AND claimed_at IS NULL` — recovery janitor.
     - Partial `notifications (claimed_at) WHERE state='pending' AND claimed_at IS NOT NULL` — sweeper.
     - `notifications (provider_message_id) WHERE provider_message_id IS NOT NULL` — webhook lookup.
     - `webhook_events (provider_message_id)` and partial `webhook_events (created_at) WHERE notification_id IS NULL` — late resolution of early events.
   - `migrate.go` exposing `Up(ctx/url)` using the embedded FS.

4. **Migration test** (`internal/adapters/postgres/migrate_test.go`, testcontainers-go Postgres)
   - Apply Up; insert a notification twice with same (idempotency_key, channel) → second fails with SQLSTATE 23505; same key different channel succeeds. Same for `webhook_events` with non-null `event_id` (NULL event_ids don't collide, noted in a comment). Skip with clear message if Docker unavailable (`testing.Short()`/provider check). Driver: pgx/v5.

5. **Local env** (`infra/`)
   - `infra/docker-compose.yml`: Postgres 16 (healthcheck), Kafka single-node KRaft, topic-init job (partitions configurable, key = idempotency_key). Makefile: `up`, `down`, `migrate` targets.

6. **Docs**: update `docs/decision_log.md` (state transitions, indexes, migration tool, testcontainers), tick Phase 1 in `docs/implementation_plan.md`, add run notes.

## Dependencies added
`github.com/jackc/pgx/v5`, `github.com/golang-migrate/migrate/v4` (pgx5 + iofs), `github.com/testcontainers/testcontainers-go/modules/postgres`.

## Verification
- `make verify` (fmt, vet, golangci-lint incl. import conventions, `-race` tests) passes.
- `go test -race ./internal/domain/...` for state machine.
- Migration test with Docker running; confirm both duplicate inserts yield 23505.
- `make up && make migrate`, then `psql \d notifications` / `\d webhook_events` to inspect constraints and indexes; `EXPLAIN` the janitor and sweeper queries to confirm partial indexes are used; `make build` and run each `cmd/*` with and without required env vars.
- Check import direction: `go list -deps ./internal/domain` shows no project packages beyond itself.
