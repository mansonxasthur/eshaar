# Phase 3: Sender — plan

## Context

Phase 2 (dispatcher) is merged. Phase 3 delivers the sender: a Kafka consumer group where each consumer saves a batch of notifications to Postgres before acknowledging Kafka, then sends each channel concurrently through a bounded pool without blocking the consume loop (`docs/implementation_plan.md`, Phase 3). Decisions made with the user: the pool **sends once and records the outcome** (`accepted` / `failed`) through a minimal mock provider; fallback, breaker and the full mock stay in Phase 4. Phase 3 branches from `origin/main`.

## Workflow (per CLAUDE.md)

1. `git fetch && git switch -c phase-3-sender origin/main`.
2. `gh issue create` "Phase 3: Sender" with the steps below as a checklist, before any code.
3. One commit per step, each preceded by `/code-review` and `make verify`; commits say `Refs #N`, the last `Closes #N`. No Co-Authored-By footer.
4. PR from the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.
5. Write `docs/sessions/2026-10-19-phase-3-sender.md`, add a "Sender" section to `docs/decision_log.md`, mark Phase 3 done in `docs/implementation_plan.md`.

## Design

**New layer `internal/sender`** (service/orchestration; imports only `domain` and `ports`, wired in `cmd/sender`). Add it to the Architecture list in `CLAUDE.md`.

**Domain** (`internal/domain/request.go`): `Request.Notifications(now) []Notification` splits a request into one `pending` row per channel (sorted channel order): only that channel's block in `Content`, `Email` on email rows, `Phone` on sms/whatsapp rows.

**Ports** (`internal/ports`, domain-only imports):
- `Consumer.Poll(ctx) (Batch, error)`; `Batch` has `Requests() []domain.Request`, `Ack(ctx) error` (commit offsets, then allow rebalance) and `Release()` (rewind to the batch start so Kafka re-delivers, then allow rebalance). The service owns the loop, so insert-then-ack is testable with a fake consumer.
- `NotificationStore`: `InsertPending(ctx, []domain.Notification) ([]domain.Notification, error)` returns **only the rows this call inserted** (conflicts omitted); `MarkAccepted(ctx, id, provider, providerMessageID)` and `MarkFailed(ctx, id, reason)`, both guarded by `state = 'pending'` so a late call cannot move a row backward (reuse `Notification.Accept/Fail` rules).
- `Provider`: `Name() string`, `Send(ctx, domain.Notification) (providerMessageID string, err error)`. Wiring builds `map[domain.Channel]ports.Provider`, so a new channel or provider is an adapter plus one map entry, with no sender change. Phase 4 turns the map value into an A/B pair.

**Sender service** (`internal/sender/sender.go`), loop per consumer:
1. `Poll` → `batch.Requests()` → flatten to notifications.
2. `InsertPending` (one statement for the batch). On error: `batch.Release()`, log, back off (capped exponential), retry. Nothing is acked.
3. `batch.Ack(ctx)` only after the insert returned. If the ack fails, the batch is redelivered and hits the unique constraint (step 2 then returns no new rows).
4. For each **newly inserted** row, acquire a slot from a semaphore (`SENDER_POOL_SIZE`), then run the send in a goroutine. A full pool blocks the acquire, so the loop stops polling and the backlog waits in Kafka (the plan's "stop reading").
5. Send goroutine: per-send timeout (`SENDER_SEND_TIMEOUT`, default 3s, inside the 5s target), call the channel's provider through an outbound limiter, then `MarkAccepted` or `MarkFailed(err.Error())`. Interim: any provider error → `failed` (Phase 4 replaces this with fallback and `unresolved`). A store error here is logged; the row stays `pending` for the Phase 6 janitor.
6. Shutdown: stop polling on ctx cancel, wait for in-flight sends (bounded drain timeout) using a context not tied to the signal; whatever is cut off stays `pending` for the janitor.

**Duplicate handling decision (record in the log):** rows that conflict on `(idempotency_key, channel)` are treated as already saved and are **not** sent again. Whoever inserted the row owns the send; if that process died, the janitor re-drives it. Sending conflict rows would double-deliver on every redelivery.

**Outbound rate limit:** `rate.Limiter` (`golang.org/x/time`, already a dependency) per provider, applied by a decorator around `ports.Provider` in `internal/sender` (`PROVIDER_RATE_LIMIT_RPS`, burst equal to RPS); `Wait(ctx)` counts against the send timeout.

**Adapters:**
- `internal/adapters/kafka/consumer.go`: franz-go group consumer on `KAFKA_TOPIC`, group `KAFKA_GROUP_ID` (default `eshaar-sender`), `DisableAutoCommit`, `BlockRebalanceOnPoll`, `PollRecords(SENDER_BATCH_SIZE)`. `Ack` = `CommitRecords` + `AllowRebalance`; `Release` = `SetOffsets` back to the first offset per partition + `AllowRebalance`. Reuse `decode` from `message.go`. A record that fails to decode is logged (topic/partition/offset, no payload) and skipped but committed with its batch, so one poison message cannot stall a partition (no DLQ: noted in the log). Verify exact franz-go calls against its docs (context7) before writing.
- `internal/adapters/postgres/store.go`: `pgxpool`; `InsertPending` is one `INSERT ... SELECT FROM unnest(text[]...)` with `content` cast `::jsonb` and nullable email/phone arrays, `ON CONFLICT (idempotency_key, channel) DO NOTHING RETURNING ...` (also handles the same key twice inside one batch). `MarkAccepted`/`MarkFailed` are single guarded `UPDATE`s that set `updated_at`, `accepted_at` etc. (no trigger, per migrations).
- `internal/adapters/mock/provider.go`: minimal always-succeeds provider (generated message id, optional injected error/delay hooks for tests). Phase 4 extends this package into the full mock.

**Config** (`internal/platform/config`): new `NeedSender` flag; `KAFKA_GROUP_ID`, `SENDER_POOL_SIZE` (default 256), `SENDER_BATCH_SIZE` (500), `SENDER_SEND_TIMEOUT` (3s), `PROVIDER_RATE_LIMIT_RPS` (1000), all validated and reported in the existing joined-error style.

**`cmd/sender/main.go`**: load config (`NeedDatabase|NeedKafka|NeedSender`), logger built once and passed down, pgxpool + store, consumer, provider map from the mock, run until SIGINT/SIGTERM, then drain and close. Run several instances to form the group.

## Steps / commits

1. Domain: `Request.Notifications` + tests.
2. Ports + `internal/sender` service with fakes + unit tests; pool and limiter.
3. Postgres store + container tests; add `pgxpool` use (no new module).
4. Kafka consumer adapter + container tests.
5. Mock provider, config additions, `cmd/sender` wiring.
6. End-to-end test, decision log, session log, plan status, `CLAUDE.md` architecture note; `Closes #N`.

## Tests

- Unit (fakes): save happens before ack (call-order recorder); insert failure → no ack, `Release`, retry, then success; ack failure → redelivery yields no second send; conflict rows are not sent; one send per new row with the right content block, email/phone; pool of 1 with a blocking provider → the second `Poll` is not made until a slot frees; provider error → `MarkFailed`; success → `MarkAccepted` with provider and message id; send timeout; limiter throttles; graceful drain; `Request.Notifications` (all channels, only own block, contact fields, Arabic bytes intact); config.
- Postgres (testcontainers, skip under `-short`/no Docker, same pattern as `migrate_test.go`): multi-row insert returns all rows; second insert returns none; same key twice in one batch inserts once; Arabic content round-trips; guarded updates refuse a non-`pending` row.
- Kafka consumer (testcontainers): `Ack` commits (a new consumer in the group sees no re-read); `Release` re-delivers the same records; poison record skipped; two consumers in a group split partitions.
- E2E (`cmd/sender/e2e_test.go`, Kafka + Postgres containers): publish a 3-channel request through `kafka.Publisher`, run the sender, assert 3 rows `accepted` with provider ids, content blocks per channel, and 3 mock sends; publish the same key again → still 3 rows and still 3 sends; kill the consumer mid-batch before ack → redelivery, no lost rows.

## Critical files

New: `internal/sender/`, `internal/ports/{consumer,store,provider}.go`, `internal/adapters/kafka/consumer.go`, `internal/adapters/postgres/store.go`, `internal/adapters/mock/provider.go`, `cmd/sender/e2e_test.go`. Modified: `cmd/sender/main.go`, `internal/domain/request.go`, `internal/platform/config/config.go`, `CLAUDE.md`, `docs/decision_log.md`, `docs/implementation_plan.md`. Reuse: `decode` in `internal/adapters/kafka/message.go`, `postgres.MigrateUp`, the `newDB`/testcontainers helpers in `migrate_test.go`, the Kafka container setup in `cmd/dispatcher/e2e_test.go`, `Notification.Accept/Fail`, `logging.New`, `config.Load` + `Need` flags.

## Verification

- `make verify` green on every commit.
- Manual: `make up && make migrate`; run `bin/dispatcher` and two `bin/sender` instances (same group); POST the decision log's Arabic three-channel sample; `psql` shows 3 rows `accepted` with `provider_message_id`, Arabic intact; repeat the POST → no new rows; stop Postgres mid-run → consumer retries and nothing is acked; with `SENDER_POOL_SIZE=1` and a slow provider, consumer lag grows in Kafka instead of memory.
- CI `verify` check green on the PR.
