# Session: Phase 5 webhooks (2026-10-19)

Issue: [#11 Phase 5: Webhooks](https://github.com/mansonxasthur/eshaar/issues/11). Plan: `_plans/phase-5-webhooks.md`.

## Flow

1. **Planning.** Read the implementation plan, the decision log and the Phase 4 code. Proposed the design in chat and asked two questions, answered yes to both: HMAC-SHA256 over the body with per-provider secrets in `providers.yaml`, and filling `provider` and `provider_message_id` on an `unresolved` row resolved by key.
2. **Issue and branch.** Created #11 with a checklist, then branched `phase-5-webhooks` from the updated `main`.
3. **Build, one commit per step.** Each step ran `/code-review` and `make verify` before its commit. Reviews ran two or three rounds per step; what was fixed and what was skipped is below.
4. **Change from the plan.** An unknown provider returns `401`, the same as a bad signature, instead of `404`, so callers cannot probe which providers exist.

| Step | Content |
| --- | --- |
| 1 | Issue and branch |
| 2 | Ports, event fields, per-provider `webhook_secret` (redacting `Secret` type) |
| 3 | Signature check and event parsing |
| 4 | Postgres store: batch insert, resolution, guarded state update, `ResolvePending` |
| 5 | Service: batching, per-event retry, resolver loop, shutdown |
| 6 | Endpoint, `cmd/webhook` wiring, `WEBHOOK_*` settings |
| 7 | Mock: `EventID`, `Event.JSON()`, `WithLateDelivery`, ids that survive a restart |
| 8 | End-to-end tests, decision log, plan status, this log; `Closes #11` |

## Review findings acted on

- **Poison events:** a body Postgres cannot store in `jsonb` (`\u0000`, a lone surrogate escape, an out-of-range exponent) would fail its whole batch and be retried forever, so the parser rejects them; a batch that fails is retried event by event so one bad event fails only itself.
- **Parser and SQL disagreed:** Go's decoder matches keys case-insensitively, the SQL reads them case-sensitively, so a body with `"STATUS"` would be saved and never applied. Keys are now matched exactly.
- **Cross-provider overwrite:** matching by key alone would let a late event from a provider a send fell over from overwrite the provider that took it. Key matching is limited to `unresolved` rows with no message id, and an event must name a channel its provider serves.
- **Unique index:** recording an event's message id on a row could collide with a row of another channel that holds it, failing the batch. The id is recorded only when no row holds it.
- **Secrets:** a secret printed by `%+v`, JSON, slog or a config error would reach the logs. `config.Secret` redacts itself, errors hide values of credential-looking variables, and a secret under 16 characters or none at all stops the process at startup.
- **Probing:** an unknown provider and a bad signature return the same response, take about the same time, and the error does not echo the caller's provider name.
- **Shutdown:** a mutex held while blocked on a full queue could delay shutdown; stop and exit channels replaced it, and the drain gives up after one flush timeout.
- **Mock ids:** the mock's counters restarted at 1 on every start, which would collide on `(provider, message id)` and drop events; ids carry a random part.
- **Skipped:** `/healthz` does not check the store; the strict `delivered` and `failed` statuses; row locks in a fixed order; classifying batch errors by SQLSTATE; the first-final-wins rule; rows accepted more than an hour after their event. The trade-offs are in the decision log.

## Verification

- `make verify` passes (fmt, vet, golangci-lint, `-race` tests).
- Unit tests: signatures (RFC 4231 vector, malformed, missing, empty key, unknown provider), parsing (statuses, exact keys, escapes, numbers, Arabic, limits), the service (batching, interval, refusals, per-event retry, timeouts, cancelled requests, full queue, shutdown, concurrency, resolver, throttle), the handler (status codes, identical `401`, body limit, routes) and the settings.
- Postgres tests with a real database: delivered and failed, duplicates, events without an id, the same id under two providers, final states staying final, the first event in a batch winning, resolution by key and by id, a row accepted by another provider never matched by key, channel mismatch, early events resolved after the accept, the lookback window, ids held by another channel, 50-event batches and concurrent batches.
- End-to-end tests with the sender, the mock provider and the real endpoint over one Postgres: a late failure ends `failed`, a late delivery ends `delivered`, an event that beats the accept is stored unlinked and applied afterwards, a duplicate changes nothing, a late event moves an `unresolved` row and names its provider, and a forged event changes nothing; the mock's JSON parses as a webhook.
- `cmd/webhook` run against a stopped database: refuses to start without a secret, and with one reaches the database connection.

## State at end of session

Commits on `phase-5-webhooks`, to be pushed with a PR that closes #11.
