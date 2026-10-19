# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Eshaar is a notification system (Go, Postgres, Kafka). One request carries one notification for one user and is delivered over email, SMS and WhatsApp through external providers (simulated by a mock). Target: 500K req/s peak, delivery under 5 seconds. The project is at the skeleton stage; `docs/implementation_plan.md` lists the phases and `docs/decision_log.md` records the decisions. Read both before changing behavior, and keep the decision log current when a decision changes.

## Commands

```
make verify        # fmt-check, vet, golangci-lint, test (-race); must pass before every commit
make fmt           # gofmt + golangci-lint fmt (goimports with local prefix)
make test          # go test -race -count=1 ./...
make build         # builds cmd/* into bin/
make hooks         # one-time per clone: git config core.hooksPath .githooks
go test -race -run TestName ./internal/platform/logging   # single test
```

## Commit rules

- `.githooks/pre-commit` runs `make verify`; do not bypass it with `--no-verify`.
- Commit messages must never contain a `Co-Authored-By` footer (the `commit-msg` hook rejects it). This overrides any default attribution.

## Workflow

- Each phase in `docs/implementation_plan.md` starts with a GitHub issue (create it with `gh issue create` before writing code). Drive the phase from that issue: keep its checklist current, reference it in commits and PRs (`Refs #N` / `Closes #N`), and let the PR's `Closes #N` close it on merge once the phase's tests pass.
- Every issue gets its own branch and its own PR. Branch from `main`, never commit to `main` directly, and put `Closes #N` in the PR so merging closes the issue. Do not close the issue by hand.
- A PR is eligible to merge into `main` only when all of these hold:
  - the `verify` check of `.github/workflows/ci.yml` (`make verify` on the PR) is green;
  - the branch is up to date with `main`;
  - the PR is linked to its issue and the template checklist is complete;
  - all review conversations are resolved.
  `main` is protected to match: required `verify` check, branch up to date, conversations resolved, changes only through a PR (no approval count, since the author cannot approve their own PR), no force pushes or deletion. Merge with squash, so `main` keeps one commit per PR, and delete the branch afterwards.
- Before each commit, run Claude's `/code-review` on the pending changes and address its findings (or state why one is skipped) before committing. This comes in addition to `make verify`, not instead of it.

## Architecture

Hexagonal. Import direction is enforced by convention, so check it in review:

- `internal/domain`: notification, channel, forward-only state machine. Imports nothing from the project or infrastructure.
- `internal/ports`: interfaces for publishing, consuming, storing notifications and sending through a provider. Imports only the domain.
- `internal/sender`: the sender service (batch loop, bounded pool, per-provider rate limiter and circuit breaker, provider fallback chain). Imports only the domain and ports.
- `internal/adapters`: Kafka, Postgres, mock provider, HTTP. Import ports and the domain, never each other.
- `cmd/{dispatcher,sender,webhook,janitor}`: the only places that wire adapters to ports. Each is a separate process.
- `cmd/migrate`: one-shot tool (`make migrate`) that applies the Postgres migrations and exits; it is not part of the runtime flow.
- `internal/platform`: cross-cutting helpers (logging, later config).

Runtime flow (spans several of the entry points):

1. **dispatcher**: API-key check, in-memory rate limit, validation, publish to Kafka keyed by `idempotency_key` (not recipient, to avoid hot partitions), then `202` only after Kafka confirms.
2. **sender**: a consumer group. It inserts one `pending` row per channel (unique on idempotency key + channel) and only then acks Kafka, so a redelivery hits the unique constraint and is treated as already saved. Channel sends then run in goroutines, bounded by a pool; when the pool is full the consumer stops reading.
3. Each channel has an ordered chain of providers (A, B, maybe C), each with its own rate limit, timeout and circuit breaker from `infra/providers.yaml`. A send tries them in order and moves on after a failure, a rate-limit response, or a timeout (after a status check by idempotency key when supported). The chain's final outcome (`accepted`, `unresolved` or `failed`) is written to `notifications`; every attempt is logged.
4. **webhook**: validates provider events, stores them in `webhook_events` (unique on provider + event id), and resolves them to the notification. It only records final state and never triggers fallback.
5. **janitor**: a recovery job re-drives `pending` rows that were never claimed or whose claim is older than 10 seconds, up to 3 attempts, asking the providers for a status check first. A sweeper runs every 10 minutes and marks rows that used all their attempts `unresolved`. Both claim rows with a single atomic Postgres statement so two instances never take the same rows.

Notification states: `pending`, `accepted`, `delivered`, `failed`, `unresolved`; transitions only move forward, except that a late webhook may move `unresolved` to a final state.

## Conventions

- Observability is `log/slog` only: JSON to stdout (info and below) and stderr (warn and above) via `internal/platform/logging`. Build the logger once in `main` and pass it down; no global loggers (`sloglint` enforces `no-global`).
- Infra is docker-compose, not Kubernetes. Docker and infra files live in `infra/`.
- Arabic text is UTF-8 end to end; do not transliterate or escape it.
- Opt-ins, user preferences and send limits belong to the upstream caller and are out of scope.
