# Session: Phase 6 janitors (2026-10-19)

Issue: [#13 Phase 6: Janitors](https://github.com/mansonxasthur/eshaar/issues/13). Plan: `_plans/phase-6-janitors.md`.

## Flow

1. **Planning.** Read the implementation plan, the decision log and the Phase 4 and 5 code, then wrote the plan in plan mode and had it approved.
2. **Issue and branch.** Created #13 with a checklist, then branched `phase-6-janitors` from `main`.
3. **Build, one commit per step.** Each step ran `/code-review` and `make verify` before its commit.
4. **Changes from the plan.** `internal/janitor` depends on a one-method `Redriver` interface and does not import `sender`; `sender.New` keeps its signature and builds its own `Deliverer`; the attempt limit is a constant, not a setting; `Resume` wraps around to the providers before `last_provider`; `janitor.New` refuses configurations where a re-drive could outlast the stale age.

| Step | Content |
| --- | --- |
| 1 | Migration 0005, domain fields, `ports.JanitorStore`, claim and sweep queries, `EXPLAIN` test |
| 2 | `sender.Deliverer` with `Send` and `Resume` |
| 3 | `internal/janitor`: recovery loop, sweeper, bounded re-drives |
| 4 | `cmd/internal/wiring`, janitor config, `cmd/janitor` |
| 5 | End-to-end tests, decision log, plan status, this log; `Closes #13` |

## Review findings acted on

- **Order promise:** `UPDATE ... WHERE id IN (... ORDER BY ...) RETURNING` does not keep the subquery order, so the port no longer promises oldest first.
- **Limit constant and SQL:** the queries are built from the attempt-limit constant; invalid limits are rejected.
- **Skipped providers lost on resume:** a provider skipped by an open breaker could sit before `last_provider` and never be tried again, while a later refusal would mark the row `failed`. Resume now wraps around.
- **Re-claim while in flight:** a re-drive longer than the stale age lets another janitor claim the row and send it twice, or the sweeper mark it `unresolved`. `janitor.New` rejects a budget over the stale age, a batch over the concurrency, and a tick over the stale age.
- **Attempts lost on shutdown:** claimed rows are finished, not dropped, since the claim already used an attempt.
- **Tight loop:** the ticker became a timer restarted after each pass.
- **Skipped:** the status-check-error path still re-sends and may duplicate, as after a timeout; a provider that refused may be retried after wrap-around; breakers and limiters are per process; a CHECK that `attempts >= 3` implies `claimed_at` is set; a compose entry (compose runs only Postgres and Kafka).

## Verification

- `make verify` passes (fmt, vet, golangci-lint, `-race` tests).
- Postgres tests: claim rules (age, null `claimed_at`, attempts, state), limit and order, sweeper rules and batching, two claimers over 400 rows, the `EXPLAIN` check that both queries use their partial indexes on 50,000 rows.
- Unit tests: `Resume` (starts at `last_provider`, falls through, wraps around, status check accepts without a send, unknown provider, ends pending again), the janitor passes, concurrency bound, panics, shutdown, sweeper batches and the config checks.
- End-to-end tests (`cmd/janitor`, real Postgres): recovery after a simulated sender crash, acceptance by status check with no second send, a never-completing row ending `unresolved` after 3 claims while others recover, and two janitors sending each of 60 rows once.

## State at end of session

Commits on `phase-6-janitors`, to be pushed with a PR that closes #13.
