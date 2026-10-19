# Notification System — Implementation Plan

Oct 19, 2026 · @Mohamed Ibrahim

## Architecture and layout

The dispatcher accepts fast, Kafka holds the work, and the sender delivers and records every outcome, with webhooks and janitors closing the gaps.

&#91;embedded content: system architecture · 8 components\]

Requests flow left to right across the top. The sender writes to the database and calls the providers, which answer later through webhooks. The janitors act on the database.

The code follows a hexagonal architecture:

- **Domain:** notification, channel and the state machine, with no knowledge of Kafka, the database or providers.
- **Ports:** interfaces for publishing, consuming, storing notifications, and sending through a provider.
- **Adapters:** Kafka, Postgres, the mock provider and the HTTP handlers.
- **Entry points:** dispatcher, sender, webhook endpoint and the two janitors, each wired to the ports it needs.

## Phase 1: Foundations

The goal is a runnable skeleton with Kafka, the database and the schema in place before any business logic.

- Create the Go repository with the hexagonal layout: domain, ports, adapters, and entry points for the dispatcher, sender and janitors.
- Add configuration loading and a local environment with Kafka and Postgres.
- Write migrations for `notifications` and `webhook_events`, including both unique constraints.
- Decide the indexes for the janitor queries and the webhook lookup in this step.
- Define the domain model: notification, channel, and the state machine with forward-only transitions.

**Tests:** unit tests for the state machine, and a migration test that checks both unique constraints reject duplicates.

**Status: done.** Run it with `make up && make migrate`; `make test` runs the state machine and migration tests (the latter needs Docker). Decisions are in the decision log under Configuration, Schema and migrations, and Local environment.

## Phase 2: Dispatcher

The goal is an HTTP endpoint that validates a request, publishes it to Kafka, and answers `202 Accepted` as soon as Kafka confirms.

- API key header middleware that validates the key and nothing else.
- In-memory rate limiting middleware.
- Request validation: idempotency key, recipient ID, notification type, per-channel content, and the email or phone required by each channel present.
- A Kafka publisher adapter behind a port, so the handler does not depend on Kafka directly. The idempotency key is the message key.
- Respond `202` only after Kafka confirms the write, and return a clear error otherwise.

**Tests:** unit tests for validation rules (including a missing phone or email), the rate limiter and the key check, and an end-to-end test from request to a message on the topic.

## Phase 3: Sender

The goal is a Kafka consumer group where each consumer saves a notification before acknowledging it, then sends to every channel concurrently without blocking the consume loop.

- Consume a message and insert one `pending` row per channel, keyed by idempotency key plus channel.
- Acknowledge Kafka only after the insert succeeds. If the insert fails, nothing is acknowledged and Kafka redelivers the message. A redelivered message hits the unique constraint and is treated as already saved.
- Store only the row's own channel block in `content`, with `email` or `phone` taken from the request.
- Run the consumers as a group, so each partition is read by one consumer in order. Each consumer saves the rows, acknowledges, then starts the channel sends in goroutines and moves on to the next message.
- Define channel and provider ports so adding a channel or provider means adding an adapter, not changing the sender.

* Save each consumed batch with one multi-row insert, and acknowledge after the whole batch is saved.
* Bound the channel sends with a pool and an outbound rate limit per provider. When the pool is full, the consumer stops reading so the backlog waits in Kafka.

**Tests:** unit tests for the insert-then-ack order and duplicate handling, and an end-to-end test that one message produces the expected rows and sends.

**Status: done.** Run it with `make up && make migrate`, then `bin/sender` (several instances form the group). The pool sends once through the mock provider and records `accepted` or `failed`; fallback and the circuit breaker are Phase 4. Decisions are in the decision log under Sender.

## Phase 4: Providers and fallback

The goal is a mock provider and the fallback logic from provider A to provider B, with a circuit breaker around each provider.

- Build a mock provider that can succeed, fail, delay, return a rate-limit response, and accept a message then send a failure webhook later.
- Give each channel an ordered chain of providers (A, B, maybe C), each with its own rate limit, call timeout and circuit breaker, configured in `infra/providers.yaml` with values from the environment.
- Run each channel send as: provider A first, and on a failure switch to the next provider immediately.
- On a timeout, make one final status check against that provider with the idempotency key if it supports it, then switch to the next. If it cannot check, switch anyway.
- On a rate-limit response, switch to the next provider without retrying.
- Wrap each provider in a circuit breaker for transient errors and timeouts only.
- Save the outcome when the chain ends: `accepted` with `provider_message_id`, `unresolved` if nothing accepted and a provider timed out, or `failed` if every provider refused. Log every attempt.

**Tests:** unit tests for each fallback branch (failure, timeout with a positive status check, timeout with no check, rate limit, both timing out, a chain of three) and for the circuit breaker opening and closing, plus an end-to-end test of the outcomes reaching the rows.

**Status: done.** Providers and policies are in `infra/providers.yaml`; `bin/sender` reads it (`PROVIDERS_CONFIG`). Decisions are in the decision log under Providers and fallback.

## Phase 5: Webhooks

The goal is an endpoint that validates provider events and records the final delivery state without triggering any fallback.

- Validate each event with the signature scheme of its provider.
- Insert the event into `webhook_events`. On a duplicate `(provider, event_id)`, skip silently and return `200 OK`.
- Resolve the notification through `provider_message_id` or the echoed idempotency key and channel, then write `delivered` or `failed`.
- Keep the event even when the notification link cannot be resolved yet, and resolve it afterward.
- Apply forward-only transitions, allowing a late event to move `unresolved` to a final state.

* Keep the endpoint thin: validate, then batch-insert events.

**Tests:** unit tests for signature validation, duplicate events and out-of-order events, and an end-to-end test where the mock provider accepts a message and later sends a failure webhook.

## Phase 6: Janitors

The goal is two background jobs that recover stalled rows and mark the ones that never complete as `unresolved`.

- Add an `attempts` column (migration 0003) that counts janitor attempts, and replace the two partial indexes of migration 0001 with `(COALESCE(claimed_at, created_at)) WHERE state = 'pending' AND attempts < 3` and `(claimed_at) WHERE state = 'pending' AND attempts >= 3`.
- **Recovery janitor, every 5 seconds:** atomically claim a batch of `pending` rows with `attempts < 3` where `claimed_at` is null and `created_at` is older than 10 seconds, or `claimed_at` is older than 10 seconds. Set `claimed_at` and increment `attempts` in the same statement.
- **Re-drive, status check first:** ask every provider in the channel's chain that supports it whether it already took the idempotency key and accept with that provider's message id if so. Otherwise run the normal fallback chain and write the outcome, guarded by `state = pending`.
- **Sweeper, every 10 minutes:** mark `pending` rows with `attempts >= 3` and `claimed_at` older than 10 seconds as `unresolved`, with a `failure_reason`.
- Make each claim a single atomic Postgres statement (UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING) so two janitor instances never take the same rows.
- Reuse the sender's delivery code: extract it into an exported `sender.Deliverer` that `cmd/janitor` wires into `internal/janitor`, and move the providers-file wiring from `cmd/sender` to a package under `cmd/internal` that both commands use. A Kafka re-publish would not work, because the sender does not send rows that already exist.
- Settings: attempt limit (3), stale age (10 seconds), janitor tick (5 seconds), sweeper tick (10 minutes), batch size and re-drive concurrency.

**Tests:** unit tests for both selection rules including the `attempts` limit and the 10-second age on the null case, the sweeper touching only exhausted rows and setting a reason, a re-drive that is accepted by the status check without a send, an end-to-end test that simulates a sender crash after the acknowledgement and verifies recovery, and a concurrency test with two janitors competing for the same rows.

## Phase 7: Load test, API docs and solution document

The goal is to prove the behavior under load and package everything for review.

- Run a scaled-down load test, measure per-instance throughput and where it saturates, and extrapolate to 500K req/s (instances needed = 500K divided by the per-instance rate). Record the results.
- Run a scenario where a provider fails or times out, and record how the system behaves.
- Have the AI agent document the API endpoints at the end of the project.
- Write the solution document using the Decision Log as the base.
- Add the link to the AI agent conversation, which the assignment treats as a key deliverable.

## Testing and delivery checklist

Every phase ships with unit tests and end-to-end tests before the next phase starts. The reviewers read the code and do not run it, so the code and the document must explain every decision on their own.

- [ ] Source code in a Git repository, readable, with meaningful tests
- [ ] Documented endpoints
- [ ] Clear module boundaries and low coupling
- [ ] Architecture diagram showing components and message flow
- [ ] Main trade-offs and why each was chosen
- [ ] Main difficulties and how they were solved
- [ ] Load test results and behavior when a provider fails
- [ ] Ideas for a different or better solution with more time or resources
- [ ] Link to the AI agent conversation
