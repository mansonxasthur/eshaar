# Session: Phase 7 load test, API docs and solution document (2026-10-19)

Issue: [#15 Phase 7: Load test, API docs and solution document](https://github.com/mansonxasthur/eshaar/issues/15). Plan: `_plans/phase-7-load-test-and-docs.md`.

## Flow

1. **Planning.** Read the implementation plan, the decision log and the Phase 6 session, found that nothing could generate load or run the processes as a stack and that the config-built mock had no latency or failure settings, and wrote the plan in plan mode.
2. **Issue and branch.** Created #15 with a checklist, then branched `phase-7-load-test-and-docs` from `main`.
3. **Build and measure, one commit per step.** Each commit ran `/code-review` and `make verify` first.
4. **Writing.** The load test and failure results were written from the actual command output, then the API reference was checked by running its examples against the stack, then the solution document was built from the decision log.

| Step | Content |
| --- | --- |
| 1 | `mock: {latency, error_rate}` in `providers.yaml`, wired to the mock |
| 2 | `cmd/loadtest`, `infra/loadtest-report.sql`, `infra/Dockerfile`, compose `stack` profile, `make` targets |
| 3 and 4 | `docs/load_test.md`, `infra/provider-failure.sh` |
| 5 | `docs/api.md` |
| 6 and 7 | `docs/solution.md`, decision log, plan status, CLAUDE.md, this log |

## Findings that changed the plan

- **The bottleneck is Postgres, not the sender or dispatcher.** The first sender run backed up Kafka at about 2,700 rows/s while Postgres was at 85 % CPU and the sender at 30 %. Counting commits (one per accepted row, no HOT updates), a larger pool (worse) and `synchronous_commit=off` (a 70x drop in median delay) pinned it to the per-row `MarkAccepted` commit. Batching the outcome writes is left as follow-up, not done in this phase, because the plan asked to measure and record.
- **The first drain check was wrong.** It waited for `pending = 0`, which is true while messages are still unread in Kafka; the runs were redone waiting for the full row count, and the 300 and 600 requests/s backlog times are marked "not measured".
- **The generator under-offered at first** (463 of 500 requests): a ticker drops ticks, so the debt is now counted from the clock.
- **The pre-commit hook hit a 10-minute timeout in the Kafka package once** (it passed in 96 s on its own and in every other run); the commit was retried with the hook on, not bypassed.

## Review findings acted on

- **Mock fault injection:** `error_rate` applies only without `WithError`, and the validation applies only to the mock kind. Skipped: a startup warning when it is on (the wiring has no logger) and injected rate-limit or timeout errors (a latency above the timeout covers timeouts).
- **Load generator:** the rate uses the offering window, not the drain time; cancelled and unbuilt requests are not samples; transport failures are not latency samples; `-workers` and `-step` are validated; nearest rank uses `math.Ceil`.
- **Stack:** one shared image, the dispatcher waits for the topic, ports bind to 127.0.0.1, every provider's latency, error rate, timeout and rps are forwarded, and `stack-up` waits for `/healthz`.
- **Failure script:** the consumers are stopped before the table is emptied, pending is counted for the run only, and the log parsing's fragility is stated in its header.
- **Skipped:** an upper bound on mock latency, and the review's claim that an environment value could inject YAML (values are limited to letters, digits and `. _ + -`).

## Verification

- `make verify` passes before each commit.
- The API examples (202, 400 with a field list, 400 for an unknown field, 401, health, a repeat with a different body keeping the first) were run against the stack.
- All numbers in `docs/load_test.md` are from runs on the stack; the commands to reproduce them are at the end of that file.

## State at end of session

Commits on `phase-7-load-test-and-docs`, to be pushed with a PR that closes #15. The AI conversation is recorded by the session logs in `docs/sessions/`, which `docs/solution.md` points to.
