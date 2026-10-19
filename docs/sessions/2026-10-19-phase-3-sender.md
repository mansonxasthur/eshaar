# Session: Phase 3 sender (2026-10-19)

Issue: [#7 Phase 3: Sender](https://github.com/mansonxasthur/eshaar/issues/7). Plan: `_plans/phase-3-sender.md`.

## Flow

1. **Planning.** Read the implementation plan, the decision log and the Phase 2 code. Asked two questions, answered as: the pool sends once through a minimal mock provider and records `accepted` or `failed` (fallback stays in Phase 4), and Phase 3 branches from `main` now that Phase 2 is merged.
2. **Issue.** Created #7 with a checklist, then branched `phase-3-sender` from `origin/main`.
3. **Build, one commit per step.** Each step ran `/code-review` (low) and `make verify` before its commit.

| Step | Content |
| --- | --- |
| 1 | `Request.Notifications`: one pending row per channel |
| 2 | Consumer, store and provider ports; the sender service and outbound limiter |
| 3 | Postgres store: batched insert, guarded outcome updates |
| 4 | franz-go group consumer: manual commit, release, poison-record skip |
| 5 | Mock provider, sender config, `cmd/sender` wiring |
| 6 | End-to-end tests, decision log, this log; `Closes #7` |

## Review findings acted on

- **Rate limiter:** the burst was truncated for fractional rates; it now rounds up.
- **Failure reason:** provider error text is stored and logged, so the port documents that errors must not carry contact details, and the stored reason is capped at 500 characters.
- **Skipped:** the review noted that rows saved by a crashed process are not re-sent on redelivery. That is the decided behavior (the janitor owns recovery), now written in the decision log. It also noted `dispatch` returning on shutdown without an error; the loop exits on its next check.
- **Lint:** `sloglint` (context scope) required the `*Context` logging calls.

## Verification

- `make verify` passes (fmt, vet, golangci-lint, `-race` tests).
- Postgres tests run against a real database: multi-row insert, redelivered rows left out, the same key twice in one batch, Arabic stored as UTF-8, outcome writes refused on a non-pending row.
- Kafka tests run against a real broker: an acked batch is not read again, a released batch comes back, a poison record is skipped and committed, two group members split the topic with no key read twice.
- End-to-end tests with both containers: one three-channel message becomes three accepted rows and three sends, and a duplicate adds neither; a failed insert acks nothing, Kafka redelivers, and the message is saved and sent once.

## State at end of session

Commits on `phase-3-sender`, to be pushed with a PR that closes #7.
