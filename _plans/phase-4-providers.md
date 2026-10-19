# Phase 4: Providers and fallback — plan

## Context

Phase 3 (sender) is merged. It sends each channel through one provider and marks any error `failed`. Phase 4 adds the fallback chain, a circuit breaker per provider, status checks on timeout, and `unresolved` outcomes (`docs/implementation_plan.md`, Phase 4). Decisions made with the user: each provider has its own call timeout from env, `SENDER_SEND_TIMEOUT` is the deadline for the whole chain; a rejection plus a timeout is `unresolved`; the breaker is hand-rolled; only the final state is stored and every attempt is logged. Mid-phase the user corrected the model: each channel has an ordered chain of providers (A, B, maybe C), each cell with its own rate limit, timeout and breaker, configured in a YAML file whose values come from the environment; status checks run after every non-final timeout.

## Workflow (per CLAUDE.md)

1. `git fetch && git switch -c phase-4-providers origin/main`.
2. `gh issue create` "Phase 4: Providers and fallback" with the steps below as a checklist, before any code.
3. One commit per step, each preceded by `/code-review` and `make verify`; commits say `Refs #N`, the last `Closes #N`. No Co-Authored-By footer.
4. PR from the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.
5. Write `docs/sessions/2026-10-19-phase-4-providers.md`, add a "Providers and fallback" section to `docs/decision_log.md`, mark Phase 4 done in `docs/implementation_plan.md`.

## Design

- **Ports:** `ports.ErrTimeout`, `ErrRateLimited`, `ErrTransient`, `ErrRejected` (providers wrap them with `%w`; anything else is transient); optional `ports.StatusChecker` found by type assertion; `NotificationStore.MarkUnresolved`.
- **Mock provider:** rate limit, scripted per-call steps (delay, error, `Took`), status check, late failure event.
- **Breaker and decorators:** `sender.WithBreaker` and `WithRateLimit`, composed as breaker around limiter around adapter, both passing `Status` through.
- **Fallback:** `sender.Route{Provider, Timeout}`; `map[Channel][]Route`; `deliver` walks the chain under `SENDER_SEND_TIMEOUT`, with the outcome rules in the decision log.
- **Config:** `infra/providers.yaml` (`PROVIDERS_CONFIG`) parsed in `internal/platform/config`, built into routes in `cmd/sender`.

## Tests

Unit tests for every fallback branch and the breaker; config tests for the YAML; an end-to-end test against Kafka and Postgres of the outcomes reaching the rows. `make verify` green on every commit.

## Critical files

New: `internal/sender/{fallback,breaker,decorate}.go`, `internal/platform/config/providers.go`, `cmd/sender/providers.go`, `infra/providers.yaml`. Modified: `internal/ports/{provider,store}.go`, `internal/adapters/mock/provider.go`, `internal/adapters/postgres/store.go`, `internal/sender/sender.go`, `cmd/sender/main.go`, `CLAUDE.md`, `docs/decision_log.md`, `docs/implementation_plan.md`.
