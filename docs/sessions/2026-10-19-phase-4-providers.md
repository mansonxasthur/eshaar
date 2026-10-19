# Session: Phase 4 providers and fallback (2026-10-19)

Issue: [#9 Phase 4: Providers and fallback](https://github.com/mansonxasthur/eshaar/issues/9). Plan: `_plans/phase-4-providers.md`.

## Flow

1. **Planning.** Read the implementation plan, the decision log and the Phase 3 code. Asked four questions, answered as: each provider has its own request timeout from env and `SENDER_SEND_TIMEOUT` becomes the deadline for the whole chain; a rejection plus a timeout ends `unresolved` (`failed` only when everything is refused outright); the circuit breaker is hand-rolled in `internal/sender`; only the final state is stored and attempts are logged.
2. **Issue.** Created #9 with a checklist, then branched `phase-4-providers` from `origin/main`.
3. **Build, one commit per step.** Each step ran `/code-review` and `make verify` before its commit.
4. **Course correction.** After the fallback commit the model changed: each channel has its own ordered chain of providers (A, B, maybe C), and every channel/provider cell has its own rate limit, timeout and breaker. The fixed `Pair{A, B}` and the global provider settings were replaced by `[]Route` per channel and a YAML policy table with values from the environment. Status checks now run after every non-final timeout.
5. **Design decisions for Phase 6 and the SLO** (recorded in the decision log, no code): the 5 s is an SLO for the first successful attempt plus failover on transient errors, and provider timeouts sit outside it; Kafka lag is not expected because sends run off the consume loop, which holds while the pool is not saturated; the recovery janitor picks `pending` rows with `attempts < 3` that had no outcome for 10 s since creation or the last claim, resumes the chain from the last provider called after a status check there, and a sweeper every 10 minutes marks exhausted rows `unresolved` with a reason. A chain that ends with a provider never tried leaves the row `pending` and records `last_provider` (migration 0003), and the janitor resumes from it.

| Step | Content |
| --- | --- |
| 1 | Provider error kinds, optional `StatusChecker`, `MarkUnresolved` (port and Postgres) |
| 2 | Mock provider: rate limit, scripted steps, status check, late failure |
| 3 | Circuit breaker decorator; the decorators keep the status check |
| 4 | Fallback flow, per-provider timeouts, config and wiring (first as an A/B pair) |
| 5 | Provider chain per channel from `infra/providers.yaml`, ordered by a `priority` field |
| 6 | End-to-end fallback test, decision log, this log; `Closes #9` |

## Review findings acted on

- **Decorators hid the status check:** `rateLimited` embeds only `ports.Provider`, so a wrapped provider stopped being a `StatusChecker` and the timeout check would silently never run. `keepStatus` passes it through, with a test.
- **Breaker races:** a call that started before the breaker changed state could close or extend it when it returned; results now carry a generation. A panicking provider releases the probe.
- **Shared breakers:** one A/B pair shared by all channels would let an SMS outage open email's breaker; every cell now has its own.
- **Status check after the deadline:** it hangs off the caller's context with its own timeout, so a provider that used the whole send deadline can still be asked.
- **Limiter waits are not timeouts:** a wait that expires before any call is a transient failure, so nothing is left `unresolved` that was never sent. A skipped provider with no earlier timeout ends `failed`.
- **YAML placeholders:** environment values are limited to letters, digits and `. _ + -` so they cannot rewrite the YAML; timeouts under 1 ms are rejected (a bare `2` would be nanoseconds); a comment-only file reports "no channels configured".
- **Skipped:** the relative default path of the providers file (a container sets `PROVIDERS_CONFIG`), the budget warning staying a warning, provider names in rows changing with the file, the same provider name on two channels (legitimate), and a status check after the shared deadline (the point of the check).

## Verification

- `make verify` passes (fmt, vet, golangci-lint, `-race` tests).
- Fallback unit tests: failure, rejection, rate limit without a retry, timeout with a positive, negative and failing status check, timeout with no check support, an open breaker, both timing out, rejected then timeout, both rejected, a chain of three, per-provider timeouts and the overall deadline.
- Breaker unit tests with a fake clock: opening, reset on success, ignoring rate limits and rejections, probe closing and reopening, one probe at a time, stale in-flight results, cancellation, panics.
- Config tests for the providers YAML: ordered chains, environment values and defaults, every problem reported, injection and unit-less numbers rejected; the shipped `infra/providers.yaml` loads and builds.
- End-to-end test with both containers: one request whose email goes to B after A's rate limit, whose SMS is accepted from A by the status check with B untouched, and whose WhatsApp ends `unresolved` after both time out.

## State at end of session

Commits on `phase-4-providers`, to be pushed with a PR that closes #9.
