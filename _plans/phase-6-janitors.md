# Phase 6: Janitors — plan

## Context

Phases 1–5 are merged. The sender already leaves rows `pending` in three cases (crash between ack and send, an outcome write that failed, a chain stopped by open breakers or the deadline with `last_provider` set), and nothing recovers them yet. Phase 6 adds the two background jobs in `docs/implementation_plan.md`: a recovery janitor (every 5 s) that re-drives stalled `pending` rows, and a sweeper (every 10 min) that marks rows which used all 3 attempts as `unresolved`. `cmd/janitor/main.go` is currently a stub; migration 0004 already dropped the old janitor indexes and is waiting for 0005.

## Workflow (per CLAUDE.md)

1. `git switch main && git pull`, then `git switch -c phase-6-janitors`.
2. `gh issue create` "Phase 6: Janitors" with the steps below as a checklist, before any code. Copy this plan to `_plans/phase-6-janitors.md` like the earlier phases.
3. One commit per step, each preceded by `/code-review` and `make verify`; commits say `Refs #N`, the last `Closes #N`. No Co-Authored-By footer.
4. PR from the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.
5. Write `docs/sessions/2026-10-19-phase-6-janitors.md`, add a Janitors section to `docs/decision_log.md` (and fix its "Phase 6 will…" wording in Indexes and Sender), mark Phase 6 done in `docs/implementation_plan.md`, add `internal/janitor` and the `cmd/internal` rule to the Architecture list in `CLAUDE.md`.

## Steps

### 1. Migration 0005, domain fields, store queries

- `0005_janitor.up.sql`: `ALTER TABLE notifications ADD COLUMN attempts integer NOT NULL DEFAULT 0` plus the two partial indexes from the spec (`(COALESCE(claimed_at, created_at)) WHERE state = 'pending' AND attempts < 3` and `(claimed_at) WHERE state = 'pending' AND attempts >= 3`); `.down.sql` drops them.
- `domain.Notification`: add `Attempts int` and `LastProvider *string`.
- `ports.JanitorStore` (new, `internal/ports/janitor.go`):
  - `ClaimStale(ctx, staleAfter, limit) ([]domain.Notification, error)`
  - `SweepExhausted(ctx, staleAfter, limit) (int, error)`

  The re-drive writes its outcome through the existing `NotificationStore.Mark*` / `RecordLastProvider`, whose `state = 'pending'` guard is the guard the spec asks for.
- `internal/adapters/postgres/janitor_store.go` on the existing `Store`:
  - Claim: `UPDATE notifications SET claimed_at = now(), attempts = attempts + 1, updated_at = now() WHERE id IN (SELECT id FROM notifications WHERE state = 'pending' AND attempts < 3 AND COALESCE(claimed_at, created_at) < now() - make_interval(secs => $1) ORDER BY COALESCE(claimed_at, created_at) LIMIT $2 FOR UPDATE SKIP LOCKED) RETURNING <row columns incl. last_provider, attempts>`. One statement, so two janitors never take the same row. This also covers "claimed_at null and created_at older" since `COALESCE` equals `created_at` then.
  - Sweep: same shape, `WHERE state = 'pending' AND attempts >= 3 AND claimed_at < now() - interval`, sets `state = 'unresolved'`, `failure_reason = 'recovery attempts exhausted; delivery unknown'`, `updated_at`; run in batches until a pass returns fewer than the limit.
- **The attempt limit is a code constant (3), not a setting.** The partial indexes hard-code `attempts < 3`; if the limit were a bind parameter, a generic plan could not prove the predicate and would lose the index. The constant lives next to the SQL and is checked against the migration by the EXPLAIN test. (This narrows the spec's "settings: attempt limit"; record it in the decision log.) `state = 'pending'` is a literal for the same reason.

### 2. Extract `sender.Deliverer`

- Move `deliver`, `attempt`, `checkStatus`, `outcome`, `kindOf`, `Route`, `failureReason`, `record` and the outcome-writing half of `send` from `internal/sender/{sender,fallback}.go` into an exported `Deliverer` (new `internal/sender/deliverer.go`). It holds the store, the chains and `DelivererConfig{SendTimeout, StatusTimeout}`; the chain-versus-timeout warning from `sender.New` moves to `NewDeliverer`.
- `Deliverer.Send(ctx, n)` is today's `send`, starting at the first provider. `Deliverer.Resume(ctx, n)` starts at the index of `*n.LastProvider` (first provider if nil, empty or no longer in the chain): if that provider is a `ports.StatusChecker`, ask first and `MarkAccepted` on a hit without sending; otherwise run the existing fallback loop over `chain[idx:]`, logging attempt numbers by true chain position. A pending outcome at resume time has no earlier timeouts (the sender returns `unresolved` for those), so skipping the earlier providers loses no information; re-write `last_provider` if it ends pending again.
- `sender.New` takes a `*Deliverer` instead of the store-plus-chains pair for sending (it keeps the store for `InsertPending`). Update `sender_test.go`, `fallback_test.go`, `cmd/sender/main.go` and `cmd/sender/e2e_test.go`.

### 3. `internal/janitor` service

Imports the domain, ports and `sender` (new allowed direction: janitor → sender; update CLAUDE.md).

- `Config{Interval 5s, SweepInterval 10m, StaleAfter 10s, BatchSize, Concurrency}`.
- `Janitor.Run(ctx)`: two goroutines, a recovery loop and a sweeper loop on tickers; each pass runs to completion before the next tick (no overlap within one instance).
- Recovery pass: `ClaimStale`, then `Deliverer.Resume` for each row with at most `Concurrency` in flight; a panic in one row is recovered and logged, the row stays claimed and is retried after the stale age. Outcome writes ignore shutdown (`WithoutCancel`), as in the sender.
- Pass sizing rule: a pass must finish within the stale age, or another janitor could re-claim rows still in flight. Default `BatchSize == Concurrency` (50), so a pass is one wave of at most `SendTimeout` + status + write; `New` warns when `ceil(batch/concurrency) × (SendTimeout + 5 s)` exceeds `StaleAfter`.
- Sweeper pass: `SweepExhausted` in batches; logs the count.

### 4. Wiring, config, `cmd/janitor`

- Move `cmd/sender/providers.go` (`buildRoutes`, `newProvider`) and its test to `cmd/internal/wiring` (exported `BuildRoutes`), used by both commands.
- `config`: add `NeedJanitor` (database, providers file, `SENDER_SEND_TIMEOUT` and `PROVIDER_STATUS_TIMEOUT` so both processes share one chain deadline) and `JANITOR_INTERVAL`, `JANITOR_STALE_AFTER`, `JANITOR_BATCH_SIZE`, `JANITOR_CONCURRENCY`, `SWEEPER_INTERVAL`. Reject `StaleAfter` shorter than the chain deadline plus status check. Extend `config_test.go`.
- `cmd/janitor/main.go`: load config, build routes, `postgres.NewStore`, `NewDeliverer`, `janitor.New`, `Run` under the signal context. No Kafka. Add a `docker-compose` entry only if the sender has one (check `infra/docker-compose.yml`; otherwise leave).

### 5. Tests

- `internal/adapters/postgres` (Docker, like the existing store tests):
  - `EXPLAIN` test: seed enough rows (e.g. 50k mostly `accepted`, a few `pending`), `ANALYZE`, assert both queries' plans use the partial indexes (index scan, not seq scan), so a changed predicate cannot silently lose its index.
  - Selection rules: unclaimed older than 10 s is claimed; unclaimed younger is not; claimed older than 10 s is claimed; claimed younger is not; `attempts = 3` is never claimed; each claim increments `attempts` and sets `claimed_at`; non-pending rows are untouched.
  - Sweeper: only `attempts >= 3` and stale `claimed_at` rows, sets `unresolved` and a reason; leaves a row whose last claim is still fresh.
  - Concurrency: two goroutines claim from the same seeded set; the union is the whole set and the intersection is empty.
- `internal/sender` (fakes): `Resume` starts at `last_provider`; resume accepted by the status check makes no `Send` call; resume with no checker sends and falls through to later providers; unknown/empty `last_provider` starts at the first; ends pending again and re-records `last_provider`.
- `internal/janitor` (fakes): a pass claims then resumes each row; concurrency bound respected; a panicking row does not stop the pass; sweeper batches until drained; tickers stop on context cancel.
- `cmd/janitor/e2e_test.go` (real Postgres + mock providers in-process): simulate a sender crash after the ack (rows inserted `pending`, nothing sent), start the janitor with a short stale age, assert rows reach `accepted`; and an exhausted row (provider always down) reaches `unresolved` through the sweeper after 3 attempts.
- Known limit to record: mock providers keep their accepted messages in process memory, so a janitor process cannot status-check a message the sender process sent through its own mock. The tests cover the status path in-process; real providers answer by idempotency key.

## Critical files

New: `internal/adapters/postgres/migrations/0005_janitor.{up,down}.sql`, `internal/adapters/postgres/janitor_store.go`, `internal/ports/janitor.go`, `internal/sender/deliverer.go`, `internal/janitor/{janitor,recover,sweep}.go`, `cmd/internal/wiring/`, `cmd/janitor/e2e_test.go`.
Modified: `internal/domain/notification.go`, `internal/sender/{sender,fallback}.go` (+ tests), `internal/platform/config/config.go` (+ test), `cmd/sender/{main,e2e_test}.go`, `cmd/janitor/main.go`, `CLAUDE.md`, `docs/{decision_log,implementation_plan}.md`.
Reused as-is: `Store.MarkAccepted/MarkFailed/MarkUnresolved/RecordLastProvider` (state-guarded), `sender.WithBreaker`/`WithRateLimit`, `config.LoadProviders`, `postgres.NewStore`, the test helpers in `store_test.go`.

## Verification

- `make verify` green on every commit (fmt, vet, golangci-lint, `go test -race`).
- `make up && make migrate` applies 0005 cleanly on a database with Phase 5 data; migrate down/up round-trips.
- Manual: run `bin/dispatcher` and `bin/sender`, kill the sender with `kill -9` right after the ack, start `bin/janitor`, and watch the stranded rows go `pending → accepted` within ~15 s; point a channel at an always-failing provider and watch rows reach `unresolved` after 3 claims and the next sweeper tick (set `SWEEPER_INTERVAL=5s` to see it quickly).
- Two `bin/janitor` instances against the same table: logs show disjoint claimed ids.
