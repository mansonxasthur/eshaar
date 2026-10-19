# Notification System — Decision Log

Oct 19, 2026 · @Mohamed Ibrahim

## Scope and context

The system accepts one notification per user per request and delivers it over email, SMS and WhatsApp through external providers, at a peak of 500,000 requests per second with delivery in under 5 seconds.

The stack is Go, Postgres and Kafka, organized as a hexagonal architecture.

- One request carries one notification for one user.
- Peak load is 500K req/s, reached when a scheduled event fires.
- Delivery target is under 5 seconds from send to delivery.
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
3. After the acknowledgement, the sender starts the channel sends in goroutines so the consume loop keeps reading, firing one concurrent request per channel, each trying provider A first.
4. On a failure, it switches to provider B immediately.
5. On a timeout, it makes one final status check against provider A using the idempotency key, if the provider supports it, and then switches to provider B. If provider A cannot answer that check, it switches to B and accepts the possible duplicate, documented as a trade-off.
6. If a provider answers with a rate-limit response, the sender switches to the other provider instead of retrying.
7. After each attempt, the outcome is saved to the notifications table.

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
| accepted\_at | Nullable |
| claimed\_at | Nullable, set when the recovery janitor claims the row |
| created\_at | Set on insert, also the first-attempt marker |
| updated\_at | General last-change time |

### State machine

- A row starts as `pending`, saved by the sender before any provider call.
- It becomes `accepted` after a provider accepts the request.
- It ends as `delivered` or `failed`, set by webhooks.
- It becomes `unresolved` when both providers time out, or when a janitor gives up on it.
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
- Claims a batch of rows where `state = pending`, `claimed_at` is null, and `created_at` is older than the recovery threshold of 30 seconds.
- The claim is a single atomic statement that sets `claimed_at = now`, so concurrent janitors never take the same rows.
- Re-drives each claimed row once per channel, reusing the same idempotency key.

### Janitor 2: sweeper

- Runs every minute.
- Marks a row `unresolved` if it is still `pending` and its `claimed_at` is more than 10 seconds old.
- Because it runs once a minute, 10 seconds is the earliest a row can be marked, not the typical time.

### Idempotency

The idempotency key is essential to the design. It dedupes Kafka redeliveries, identifies the notification to providers, and makes janitor re-drives safe.

### Provider protection

- One circuit breaker per provider, for transient errors and timeouts only.
- A provider rate-limit response is not retried. The sender switches to the other provider.

## Scaling to 500K/s

The design keeps save-before-send, because a send that succeeds without a saved row would be resent on every channel. That makes the database the main bottleneck, so the design scales it on paper while the load test runs scaled down.

- **Write volume:** each notification costs about three writes (the insert, `accepted`, and the final state from the webhook). That is roughly 1.5M writes per second with one channel per request, and up to 4.5M with three.
- **Batched inserts:** each consumer saves a batch with one multi-row insert and acknowledges after the whole batch is saved.
- **Sharding (proposal):** Postgres is sharded by a hash of the idempotency key. This is documented, not built.
- **Bounded sends:** a bounded pool and an outbound rate limit per provider. When the pool is full the consumer stops reading, so the backlog waits in Kafka.
- **Webhook ingest:** the endpoint stays thin, validating and then batch-inserting events, with a retention plan for `webhook_events`.
- **Dispatcher and Kafka:** the dispatcher runs as many instances behind a load balancer, the producer batches and compresses, and the partition count is set high enough for the consumer group.
- **Janitor under load:** a healthy row can wait longer than 5 seconds in the bounded send pool, and recovery would then send it twice. The recovery threshold is 30 seconds, longer than the worst-case queue wait, and is adjusted from the load test. An in-flight marker was rejected because it adds a write per notification. A partial index on pending rows keeps the janitor query fast.
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
| Janitor re-drives each row once | Rows that still stall become `unresolved` instead of retrying indefinitely |
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
