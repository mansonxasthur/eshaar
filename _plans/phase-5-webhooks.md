# Phase 5: Webhooks — plan

## Context

Phase 4 (providers and fallback) is merged. Phase 5 adds the endpoint that validates provider events and records the final delivery state without triggering any fallback (`docs/implementation_plan.md`, Phase 5). Decisions made with the user: each event is signed with an HMAC-SHA256 of the raw body using a per-provider secret kept in `infra/providers.yaml`; when a late event resolves an `unresolved` row by idempotency key, the provider and message id are written onto the row.

## Workflow (per CLAUDE.md)

1. `git switch main && git pull`, then `git switch -c phase-5-webhooks`.
2. `gh issue create` "Phase 5: Webhooks" with the steps below as a checklist, before any code.
3. One commit per step, each preceded by `/code-review` and `make verify`; commits say `Refs #N`, the last `Closes #N`. No Co-Authored-By footer.
4. PR from the template, `Closes #N`, ending with the Claude Code attribution line; squash-merge after `verify` is green.
5. Write `docs/sessions/2026-10-19-phase-5-webhooks.md`, update the Webhooks section of `docs/decision_log.md`, mark Phase 5 done in `docs/implementation_plan.md`.

## Design

- **No migration:** `webhook_events` already has the columns, the unique `(provider, event_id)` constraint and the index for unresolved events.
- **Layers:** `internal/webhook` (service, imports only the domain and ports), `ports.WebhookStore` and `ports.EventReceiver`, the endpoint in `internal/adapters/httpapi`, the store on the Postgres `Store`, wiring in `cmd/webhook`.
- **Endpoint:** `POST /v1/webhooks/{provider}`, signature in `X-Signature`, secret per provider from `providers.yaml`. A wrong signature and an unknown provider get the same `401`.
- **Ingest:** a batcher saves events with one statement that also resolves each to its notification and applies `delivered` or `failed` from `accepted` or `unresolved`. `200` only after the commit.
- **Resolution:** by `(provider, message id)`; else by key and channel for an `unresolved` row with no message id. A resolver loop links events that arrived before the accept.
- **Mock:** `EventID`, `Event.JSON()` and `WithLateDelivery` next to `WithLateFailure`.

## Tests

Unit tests for signatures, parsing, the batcher, shutdown and the handler; Postgres tests for resolution, duplicates, out-of-order events and concurrency; an end-to-end test where the sender and the mock provider report events to the real endpoint. `make verify` green on every commit.

## Critical files

New: `internal/webhook/{verifier,event,service,flush,resolve,throttle}.go`, `internal/ports/webhook.go`, `internal/adapters/httpapi/webhook.go`, `internal/adapters/postgres/webhook_store.go`, `cmd/webhook/main.go`. Modified: `internal/domain/webhook_event.go`, `internal/platform/config/{config,providers}.go`, `infra/providers.yaml`, `internal/adapters/mock/provider.go`, `CLAUDE.md`, `docs/decision_log.md`, `docs/implementation_plan.md`.
