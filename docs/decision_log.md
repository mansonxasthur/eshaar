# Notification System — Decision Log

Oct 19, 2026 · @Mohamed Ibrahim

## Scope and context

The system accepts one notification per user per request and delivers it over email, SMS and WhatsApp through external providers, at a peak of 500,000 requests per second with delivery in under 5 seconds.

The stack is Go, Postgres and Kafka, organized as a hexagonal architecture.

- One request carries one notification for one user.
- Peak load is 500K req/s, reached when a scheduled event fires.
- Delivery target: 5 seconds is an SLO for the first successful attempt, including failover to the next provider after a transient error. Provider timeouts are bounded by the provider and sit outside it, so the SLO can be exceeded in those cases, and the janitors recover what it misses. `SENDER_SEND_TIMEOUT` caps one chain of attempts; it is not a delivery guarantee.
- Providers are simulated by a mock that can succeed, fail, delay, or accept a message and later report failure by webhook.
- Opt-ins, user preferences and send limits belong to the upstream service that calls us, and are out of scope here.

## Request contract

Every request is authenticated by an API key header only, validated by the dispatcher, and rejected with a 400 if a channel is present without its contact field.

| Field | Rule |
| --- | --- |
| idempotency\_key | Required, identifies the notification |
| recipient\_id | Required |
| notification\_type | Required |
| email | Required when content has an email channel |
| phone | Required when content has sms or whatsapp |
| content | One block per channel the user opted into |

The caller sends only the channels the user opted into. Sample content:

```json
"content": {
  "email": { "template_id": "match_started_v2", "variables": { "team_a": "الهلال", "team_b": "النصر" } },
  "sms": { "text": "مباراة الهلال والنصر تبدأ الساعة 8 مساءً" },
  "whatsapp": { "template_id": "match_started", "variables": { "team_a": "الهلال", "team_b": "النصر" } }
}
```

Decisions:

- Auth is an API key header validated and nothing else. Permissions are dropped.
- Rate limiting is an in-memory middleware. With several dispatcher instances each limits independently, so the effective global limit is per instance times the instance count. Accepted as a trade-off.
- Email and WhatsApp carry a template ID plus variables. Only SMS carries free text.
- Arabic text passes through as UTF-8 end to end.

## Flow decisions

The dispatcher accepts fast and the sender delivers asynchronously, with Kafka as the queue between them. The code follows a hexagonal architecture so channels and providers are replaceable adapters.

1. The dispatcher validates the request, publishes it to Kafka, and returns `202 Accepted` once Kafka confirms.
2. A sender consumer, one of a consumer group where each consumer reads its own partitions in order, consumes the message and saves one row per channel in state `pending`. After the save succeeds, it acknowledges the message to Kafka. If the insert fails, nothing is acknowledged and Kafka redelivers the message.
3. After the acknowledgement, the sender starts the channel sends in goroutines so the consume loop keeps reading, firing one concurrent request per channel, each trying the channel's first provider (A) first. A channel has an ordered chain of providers (A, B, maybe C), each with its own policy.
4. On a failure, it switches to the next provider in the chain immediately.
5. On a timeout, it makes one final status check against that provider using the idempotency key, if the provider supports it, and then switches to the next. If the provider cannot answer that check, it switches anyway and accepts the possible duplicate, documented as a trade-off. The last provider in the chain is not checked, since there is nowhere left to switch to.
6. If a provider answers with a rate-limit response, the sender switches to the next provider instead of retrying.
7. When the chain ends, the outcome is saved to the notifications table: `accepted`, `unresolved` or `failed`. Each attempt is logged (see Providers and fallback).

**Kafka partitioning:** the dispatcher uses the idempotency key as the message key, which spreads load evenly across partitions. Ordering between a user's notifications is not a requirement, so the recipient ID is not used as the key and a very active user cannot create a hot partition. Consumers scale with the partition count, since each partition is read by one consumer.

## Data model

There is one `notifications` row per channel, keyed by the idempotency key plus the channel, and a separate `webhook_events` table that stores every incoming provider event.

### notifications

| Column | Notes |
| --- | --- |
| id | Row identifier |
| idempotency\_key | Unique together with channel |
| recipient\_id | From the request |
| notification\_type | From the request |
| channel | email, sms or whatsapp |
| content | JSON, only this channel's block |
| email | Nullable, used by email rows |
| phone | Nullable, used by SMS and WhatsApp rows |
| state | pending, accepted, delivered, failed or unresolved |
| failure\_reason | Nullable |
| provider\_message\_id | Nullable, set when a provider accepts |
| last\_provider | Nullable, the last provider a send actually called on a row that stayed `pending` (nothing accepted it, none timed out, and some provider was never tried). The janitor resumes the chain from it. Not the accepting provider, which is `provider`. Added by migration 0003 |
| accepted\_at | Nullable |
| claimed\_at | Nullable, set when the recovery janitor claims the row |
| attempts | Recovery attempts by the janitor, starts at 0 and caps at 3. The sender's first send is not counted. Added by the Phase 6 migration |
| created\_at | Set on insert, also the first-attempt marker |
| updated\_at | General last-change time |

### State machine

- A row starts as `pending`, saved by the sender before any provider call.
- It becomes `accepted` after a provider accepts the request.
- It ends as `delivered` or `failed`, set by webhooks.
- It becomes `unresolved` when no provider accepted and at least one timed out, or when a janitor gives up on it.
- Transitions only move forward. A late or duplicate event cannot overwrite a final state.
- An `unresolved` row can still become `delivered` or `failed` if a late webhook arrives.
- A row goes straight from `pending` to `failed` when every provider rejects the send outright (not a timeout), with `failure_reason` set.
- A row never goes from `pending` to `delivered`, or from `accepted` to `unresolved`. A webhook that beats the `accepted` write is stored with no `notification_id` and re-resolved by the webhook phase once the row is accepted. Only `pending` rows are swept to `unresolved`.
- Applying the state a row is already in is a no-op (`ErrNoChange`), so duplicate webhooks and re-drives are not errors.

### webhook\_events

| Column | Notes |
| --- | --- |
| id | Row identifier |
| provider | Who sent the event, drives signature validation |
| channel | Kept separate because one provider can serve several channels |
| event\_id | Nullable, the provider's event identifier |
| provider\_message\_id | Main link back to the notification |
| notification\_id | Nullable direct reference, resolved when possible |
| payload | Raw event body |
| created\_at | Set on insert |

### Constraints

- `notifications`: unique on `(idempotency_key, channel)`. This dedupes Kafka redeliveries and janitor re-drives.
- `webhook_events`: unique on `(provider, event_id)`. Event IDs are scoped by provider, so two providers can reuse the same value.

## Reliability

Two janitors recover rows that stall after the sender acknowledges Kafka, and each provider sits behind a circuit breaker.

### Janitor 1: recovery

- Runs every 5 seconds.
- Claims a batch of rows where `state = pending`, `attempts < 3`, and either `claimed_at` is null and `created_at` is older than 10 seconds, or `claimed_at` is older than 10 seconds. Both halves mean "no outcome in the 10 seconds since the row was created or last claimed", which is `COALESCE(claimed_at, created_at) < now() - 10 seconds`. A row just inserted, and still waiting in its sender's pool, is left alone for 10 seconds.
- The claim is a single atomic statement that sets `claimed_at = now` and `attempts = attempts + 1`, so concurrent janitors never take the same rows.
- Re-drives each claimed row with the same idempotency key, resuming the channel's chain from `last_provider` (from the first provider if it is empty, since nothing was called, or if the name is no longer in the channel's chain because the providers file changed; a later run that calls nothing keeps the earlier value). The providers before it were already tried, and asked for a status check if they timed out, by the first run. At the resume provider it first asks, if the provider supports a status check, whether it already took the key; if it did, the row is accepted with that provider and message id and nothing is sent. Otherwise it sends through that provider and fails over to the later ones in the usual way. The outcome is written guarded by `state = pending`, so a row the original sender finished in the meantime is not overwritten, and a re-drive that again ends without an outcome records its own `last_provider`.
- The attempt limit (3), the stale age (10 seconds) and the tick are settings.

### Janitor 2: sweeper

- Runs every 10 minutes.
- Marks a row `unresolved` if it is still `pending`, has used all its attempts (`attempts >= 3`), and its `claimed_at` is more than 10 seconds old, so the last attempt has had its chance. It sets `failure_reason` (for example "no outcome after 3 recovery attempts").
- A stalled row therefore takes at most 3 x (10 seconds + the 5-second tick) to exhaust its attempts, then up to 10 minutes for the next sweep: about 10 minutes 45 seconds in the worst case. The sweeper is the final backstop, not a latency bound.
- A row can still be re-driven while its original send is in flight, if it waited longer than 10 seconds in a saturated pool. The status check narrows that for providers that can answer, and a pool backlog that long is itself the signal to add capacity.

### Idempotency

The idempotency key is essential to the design. It dedupes Kafka redeliveries, identifies the notification to providers, and makes janitor re-drives safe.

### Provider protection

- One circuit breaker per provider on each channel (so channels x providers breakers), for transient errors and timeouts only.
- A provider rate-limit response is not retried. The sender switches to the next provider.

## Scaling to 500K/s

The design keeps save-before-send, because a send that succeeds without a saved row would be resent on every channel. That makes the database the main bottleneck, so the design scales it on paper while the load test runs scaled down.

- **Write volume:** each notification costs about three writes (the insert, `accepted`, and the final state from the webhook). That is roughly 1.5M writes per second with one channel per request, and up to 4.5M with three.
- **Batched inserts:** each consumer saves a batch with one multi-row insert and acknowledges after the whole batch is saved.
- **Sharding (proposal):** Postgres is sharded by a hash of the idempotency key. This is documented, not built.
- **Bounded sends:** a bounded pool and an outbound rate limit per provider. When the pool is full the consumer stops reading, so the backlog waits in Kafka.
- **Webhook ingest:** the endpoint stays thin, validating and then batch-inserting events, with a retention plan for `webhook_events`.
- **Dispatcher and Kafka:** the dispatcher runs as many instances behind a load balancer, the producer batches and compresses, and the partition count is set high enough for the consumer group.
- **Janitor under load:** a healthy row can wait longer than 10 seconds in the bounded send pool when the pool is saturated, and recovery would then send it again. The 10-second window is a decision, not a proof: the status check before each re-drive removes the duplicate for providers that can answer, and the attempt limit bounds the rest. An in-flight marker was rejected because it adds a write per notification. Partial indexes on pending rows keep the janitor queries fast.
- **Kafka lag:** the sends run in goroutines off the consume loop, so lag is not expected. That holds while pool slots divided by send latency stays above the arrival rate. The loop still stops reading when the pool is full, so pool saturation and consumer lag are the signals to watch, and the janitors cannot see lag at all because those messages have no row yet.
- **Load test:** the machine cannot reach 500K/s, so the test runs scaled down, measures per-instance throughput and where it saturates, and extrapolates. Instances needed = 500K divided by the per-instance rate.

## Webhooks

Webhooks only record the final delivery state. They never trigger a fallback.

- The endpoint sits on the API entry point and validates each event by provider, using that provider's signature scheme.
- An authentic event is stored in `webhook_events` and its final state is written to the notification row.
- A duplicate event, detected by the unique `(provider, event_id)` constraint, is skipped silently and the endpoint returns `200 OK`.
- The link from an event to a notification relies on the provider echoing the idempotency key or returning a message ID. The mock provider does this, and the document states it as an assumption for real providers.
- An event can arrive before `provider_message_id` is saved on the notification. The event is still stored, and the link is resolved afterward.
- Late or out-of-order events cannot move a row backward because transitions only go forward.

## Trade-offs and scope

These are the compromises accepted on purpose, and the items left to other services.

| Decision | Trade-off accepted |
| --- | --- |
| Fallback to provider B when provider A times out and cannot report status | A notification may be delivered twice |
| In-memory rate limiter | Limits are per dispatcher instance, not global |
| Janitor re-drives a row up to 3 times, resuming from the last provider called and checking its status first | A provider that cannot report status may still deliver a re-driven row twice; rows that exhaust the attempts wait up to the 10-minute sweeper before they become `unresolved` |
| Webhooks do not trigger fallback | A provider that accepts and later reports failure leaves the row `failed` |
| Webhook-to-notification link depends on the provider echoing a reference | Real providers may need a different matching key |

Out of scope:

- Opt-in and opt-out records, subscriptions and send limits, handled by the upstream service.
- Request permissions beyond API key validation.
- Real provider integrations.

## Reserved for the solution document and interview

These topics are deliberately deferred and will be covered in the solution document or discussed in the interview.

- **Table write rate:** at 500K req/s, each notification means at least an insert plus one or two state updates, so roughly 1M or more writes per second. This drives storage choice and partitioning.
- **Kafka:** partitioning, consumer scaling, and what happens when the sender falls behind. The code has the base setup, and the document proposes how to scale it.
- **Timeout fallback trade-off:** the possible duplicate when provider A cannot report status.
- **Load test results** and system behavior when a provider fails.
- **PII:** contact details are stored per channel row, which is acceptable here but worth a note.
- **Alternatives:** how the problem could be solved differently or better with more time or resources.

## Configuration

- Settings come from environment variables only (`DATABASE_URL`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `HTTP_ADDR`, `LOG_LEVEL`), which suits docker-compose and keeps secrets out of files.
- One `Config` type is shared, but each process names what it needs (`NeedDatabase`, `NeedKafka`, `NeedHTTP`). Unneeded settings are neither required nor validated, so the janitor starts without Kafka and the dispatcher without Postgres.
- `KAFKA_TOPIC` defaults to `notifications` and `HTTP_ADDR` to `:8080`. The database URL and brokers have no default, so a missing one fails at startup with every problem listed.

## Schema and migrations

- Migrations are plain up/down SQL files embedded in `internal/adapters/postgres` and applied with golang-migrate (`MigrateUp`), so any process or test can bring a database up to date.
- The migration test starts a real Postgres with testcontainers-go and skips under `-short` or without Docker. It checks both unique constraints, that the same value under another channel or provider is accepted, the CHECK constraints, and a down-then-up round trip.
- `notifications` also has CHECKs on `channel`, `state`, and on contact fields (email rows need an address and no phone, SMS and WhatsApp rows the reverse), so a malformed row fails at insert.
- `notifications` gains a nullable `provider` column, set when a provider accepts. Message ids are only unique per provider, so webhook lookup is by `(provider, provider_message_id)`, backed by a unique partial index. An `accepted` row must have `provider`, `provider_message_id` and `accepted_at`.
- `updated_at` has no trigger; every UPDATE sets it. There is no per-attempt history table: only the chain's final state is stored, and each attempt is logged (see Providers and fallback).
- `webhook_events.event_id` stays nullable. Postgres treats NULLs as distinct, so events without an id are never deduplicated by the unique constraint.

### Indexes

| Index | Serves |
| --- | --- |
| `notifications (COALESCE(claimed_at, created_at)) WHERE state = 'pending' AND attempts < 3` | Recovery janitor claim query |
| `notifications (claimed_at) WHERE state = 'pending' AND attempts >= 3` | Sweeper query |
| `notifications (provider, provider_message_id) WHERE provider_message_id IS NOT NULL` unique | Webhook lookup by provider message id |
| `notifications (idempotency_key, channel)` unique | Webhook lookup by key and channel, plus dedup |
| `webhook_events (provider, provider_message_id)` | Matching events to a notification once it is accepted |
| `webhook_events (notification_id) WHERE notification_id IS NOT NULL` | Events of one notification, FK checks |
| `webhook_events (created_at) WHERE notification_id IS NULL` | Re-resolving events that arrived before the accept |

The janitor and sweeper indexes are partial, so they only hold the rows still waiting, not the whole table. They are split by `attempts`: rows with attempts left go to the recovery index, exhausted rows to the sweeper. The recovery index is on `COALESCE(claimed_at, created_at)`, the time a row last had a chance to finish, so one index serves both halves of the claim condition and the query must use the same expression. The Phase 6 migration (0004; migration 0003 added `last_provider`) adds `attempts`, creates these two indexes and drops the two that migration 0001 created.

## Local environment

- `infra/docker-compose.yml` runs Postgres 16 and a single-node Kafka in KRaft mode, with `kafka-init` creating the topic (`KAFKA_TOPIC`, default `notifications`, `KAFKA_PARTITIONS`, default 12) and topic auto-creation off. Kafka advertises `localhost:9092` to the host and `kafka:19092` to other containers.
- `make up`, `make down` and `make migrate` drive it. `make migrate` runs `cmd/migrate`, a small fifth entry point that applies the migrations and exits, so migrations are an explicit step and not a side effect of starting a service.

## Dispatcher

- **Endpoint:** `POST /v1/notifications` behind the API key check and the rate limiter, plus an open `GET /healthz`. The body is decoded strictly (unknown fields, trailing data and bodies over 512 KiB are rejected, kept under Kafka's 1 MB record limit) and checked by `domain.Request.Validate`, which returns every violation as a field list in one 400.
- **Validation:** `idempotency_key` (no surrounding whitespace, at most 255 bytes, since it is the Kafka key and an indexed column), `recipient_id`, `notification_type`, at least one channel, `email` for an email block, `phone` for sms or whatsapp. SMS blocks need `text`; email and WhatsApp blocks need `template_id` and may carry `variables`. Blocks reject unknown keys so a typo fails here and not at a provider. Text fields and content must be valid UTF-8 without NUL, which Postgres cannot store, so a 202 never promises a row the sender cannot insert. Email and phone formats are not checked: providers do that, and the caller owns the contact data.
- **API keys:** `DISPATCHER_API_KEYS` (comma-separated). Keys are compared by SHA-256 against every configured key, so timing does not reveal which matched. Keys are never logged.
- **Rate limit:** one in-memory token bucket per API key (`RATE_LIMIT_RPS`, default 1000, and `RATE_LIMIT_BURST`, default 2000), built at startup, so there is no lock on the hot path and unauthenticated traffic cannot grow it. Rejections are `429` with `Retry-After`. Per instance, as already accepted above.
- **Kafka client:** franz-go (pure Go, so `CGO_ENABLED=0` builds hold; good batching and consumer groups for Phase 3). The producer uses acks from all in-sync replicas, which also keeps franz-go's idempotent producer on, Snappy compression and a 5 ms linger. The linger costs up to 5 ms per request and lets concurrent requests share a batch.
- **202 only after Kafka confirms:** `Publish` is synchronous. A record that cannot be delivered within 10 s fails (so a dead broker cannot hang requests or fill the producer buffer), and the handler waits at most 1 s, then answers `503` with a generic body; details go to the log.
- **Publish errors mean "maybe stored":** a timeout can fire after the write lands. A caller that retries with the same idempotency key can create a second message, which the sender dedupes through the `(idempotency_key, channel)` unique constraint. The dispatcher does no dedup of its own.
- **Message format:** the Kafka value is the JSON of `domain.Request`, with non-ASCII text kept as UTF-8 and no version field. A schema version is left for later, when the sender consumes it.

## Sender

- **Layers:** `internal/sender` is the service. It imports only the domain and the ports (`Consumer`/`Batch`, `NotificationStore`, `Provider`) and `cmd/sender` wires the Kafka, Postgres and mock adapters to it. The service owns the loop, so insert-then-ack is tested with fakes and no broker.
- **Order per batch:** poll, split every request into one `pending` row per channel (`Request.Notifications`), insert the whole batch with one statement, ack, then start the sends. If the insert fails the batch is released (the consumer rewinds to the batch's first offsets), nothing is acked, and the next poll returns the same records after a capped exponential backoff.
- **Insert:** one `INSERT ... SELECT FROM unnest(...) ON CONFLICT ON CONSTRAINT notifications_idempotency_channel_key DO NOTHING RETURNING`, so the statement text does not grow with the batch. The same key twice in one batch inserts once.
- **Conflicting rows are not sent.** Only the rows this insert created are sent. A row that already exists was saved by an earlier delivery, and that delivery owns its send; if its process died, the Phase 6 janitor re-drives it. Sending conflicting rows would deliver every redelivered message twice. The cost is that a crash between ack and send leaves rows `pending` until the janitor's 10 s window passes. If the ack itself fails, the sender still sends the rows it just saved, since the redelivered batch would not.
- **Kafka consumer:** franz-go group consumer with auto-commit off and `BlockRebalanceOnPoll`. `Ack` commits the batch's records and lets a rebalance proceed; `Release` rewinds with `SetOffsets`. A new group starts at the beginning of the topic. A record that cannot be decoded is logged (topic, partition, offset, no payload) and skipped, then committed with its batch, so one poison message cannot stall a partition. There is no dead-letter topic.
- **Bounded pool:** a semaphore of `SENDER_POOL_SIZE` (default 256). The loop blocks on it after the ack, so a full pool stops the next poll and the backlog waits in Kafka. Rebalances are allowed at ack time, so a blocked loop does not hold up the group.
- **Outbound rate limit:** a token bucket per provider on each channel (`rps` in the providers file, burst equal to the rate rounded up), applied as a decorator around the provider. The wait counts against the send timeout and holds a pool slot, so a throttled provider also backs the loop up into Kafka.
- **Send outcome:** the fallback flow (see Providers and fallback) ends in `accepted` with the provider and message id, `failed`, or `unresolved`, the last two with a reason capped at 500 characters. Provider errors must not carry the recipient's email or phone. The writes only touch rows still `pending`. If the outcome write itself fails, the row stays `pending` for the janitor.
- **Shutdown:** the loop stops polling, then waits up to the send timeout plus 5 s for in-flight sends. Sends and their outcome writes ignore the shutdown signal so a sent notification is still recorded. Rows never started stay `pending`.
- **Settings:** `KAFKA_GROUP_ID` (`eshaar-sender`), `SENDER_POOL_SIZE`, `SENDER_BATCH_SIZE` (500, records per poll), `SENDER_SEND_TIMEOUT`, `PROVIDER_STATUS_TIMEOUT` and `PROVIDERS_CONFIG`, described under Providers and fallback.
- **Mock provider:** `internal/adapters/mock` simulates a provider; see Providers and fallback.

## Providers and fallback

- **A chain per channel:** each channel has a chain of providers (A, B, maybe C) ordered by a `priority` number, 1 first, unique within the channel and independent of the order in the file, and each provider on each channel has its own rate limit, call timeout and circuit breaker, so the policy is a channels x providers table. `sender.Route{Provider, Timeout}` is one entry, and the sender takes `map[Channel][]Route`.
- **Configuration:** `infra/providers.yaml` (path in `PROVIDERS_CONFIG`) lists the chain per channel with `name`, `priority` (1 first, unique per channel), `kind` (only `mock` exists), `rps`, `timeout` and `breaker.failures`/`breaker.open_for`. Values come from the environment through `${NAME}` and `${NAME:-default}` placeholders, replaced before the YAML is read. An environment value or default may hold only letters, digits and `. _ + -`, so it cannot change the YAML around it; an unset or empty variable takes the default, or is an error without one. Unknown fields are rejected, timeouts under 1 ms are rejected (a bare `2` would be 2 nanoseconds), and every problem is reported at once. `cmd/sender` also requires every supported channel to be configured. The default path is relative to the repository root, so a container needs the file mounted and `PROVIDERS_CONFIG` set.
- **Wiring:** each cell gets its own adapter, breaker and limiter, with the breaker outside the limiter, so an open breaker fails fast without waiting for or using a limiter token. The limiter's own wait errors are transient for the fallback but neutral for the breaker, so a throttled sender never opens a healthy provider's breaker. A panic in a provider is recovered in the send goroutine and leaves the row `pending` for the janitor. A provider's name is stored in the row's `provider` column on accept, so webhooks match on it.
- **Reordering:** the sender logs each channel's effective chain at startup, since priorities can come from the environment. `last_provider` is a provider name, so if priorities change while rows are pending, the janitor resumes from that provider's new position; rows already tried earlier in the old order may be tried again, which the status check at the resume provider narrows. Change priorities when few rows are pending.
- **Flow:** try the providers in priority order. The next provider in line is the one with the next higher priority number. A failure, a rate-limit answer or an open breaker moves to the next provider with no retry. A timeout asks that provider for a status check (its own `PROVIDER_STATUS_TIMEOUT`, default 500 ms, not rate limited or counted by the breaker): if it took the message, the row is `accepted` with that provider's message id and nothing else is called; if not, or it cannot answer, the next provider is tried and a duplicate is possible. The last provider is not asked.
- **Outcome when the chain ends:** `accepted` as above. Otherwise `unresolved` if any attempt timed out, because the message may have been delivered. If no attempt timed out but some provider was never tried (its breaker was open, or the deadline had passed), no outcome is written and the row stays `pending` for the janitor to re-drive, because nothing was sent and `failed` would lose it. `failed` is only for a chain where every provider was tried and refused outright. A rejection plus a timeout is therefore `unresolved`. The reason lists each provider's error. A late webhook can still move `unresolved` to a final state; the sender never does.
- **Error kinds:** providers wrap `ports.ErrTimeout`, `ErrRateLimited`, `ErrTransient` or `ErrRejected`. A plain deadline counts as a timeout, and any other error as transient. A status check is an optional `ports.StatusChecker` found by type assertion, and the rate-limit and breaker decorators pass it through.
- **Breaker:** hand-rolled in `internal/sender` (no dependency) with an injectable clock. After `failures` consecutive transient errors or timeouts it fails fast with `ErrCircuitOpen`; after `open_for` it lets one probe through, and a healthy answer closes it while a failure reopens it. A rate limit or rejection counts as a healthy answer, a cancelled context as neither, and a call that started before the breaker changed state cannot move it when it returns.
- **Changed settings:** `PROVIDER_RATE_LIMIT_RPS` and the per-provider timeouts are now fields of `infra/providers.yaml`, and `cmd/sender` warns at startup if the old rate-limit variable is still set. `SENDER_SEND_TIMEOUT` used to cap one provider call (3 s) and now caps the whole chain (4.5 s). The shutdown drain waits for that plus 10 s, enough for a status check past the deadline and the outcome write.
- **Time budget:** `SENDER_SEND_TIMEOUT` (default 4.5 s) is the deadline for the whole chain, set so a healthy first attempt plus one failover fits the 5 s SLO, and each provider call also has its own timeout, so a slow provider cannot use the whole budget when its timeout is shorter. The defaults (2 s + 0.5 s status + 2 s) fill the budget exactly; `New` logs a warning per channel when the timeouts and status checks add up to more than the deadline, and then a slow provider leaves the next one no time (that provider is skipped as "no time left"). The status check runs under its own timeout even after the send deadline has passed.
- **Rows left pending:** when some provider was never tried and none timed out, `send` writes no outcome, records the last provider it actually called in `last_provider` (nothing if none was called), and logs a warning. Until the Phase 6 janitor exists such a row simply waits, as one whose outcome write failed does; once it exists, its attempt limit of 3 and the sweeper bound the retries, so open breakers cannot cause an endless loop.
- **Attempts are logged, not stored:** every attempt writes a structured log line (attempt number, provider, outcome kind, duration, error) next to the notification id, idempotency key and channel. A failed attempt logs at warn; a successful one logs at debug, because it is the common case on a hot path, and which provider accepted is in the row's `provider` column. Only the final state reaches Postgres, so there is no attempts table and no schema change. The earlier wording "every attempt's outcome is written to `notifications`" meant the outcome of the chain.
- **Mock provider:** `internal/adapters/mock` can accept, fail with a chosen error, delay (ending with the caller's context, as `ports.ErrTimeout`), answer with a rate limit, follow a script of per-call steps, take a message and lose the reply (`Took`), answer status checks (or error, or be wrapped to have none), and report a failure webhook after accepting (`WithLateFailure`, consumed by the Phase 5 receiver). It echoes the idempotency key and message id in those events.
