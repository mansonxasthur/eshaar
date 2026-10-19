# API

Eshaar has two HTTP services. The **dispatcher** accepts notification requests. The **webhook** service receives delivery reports from providers. There is no read API: the caller learns the result of a notification from its own webhook consumers or from the database, and "accepted" in the dispatcher's answer means "stored in Kafka", not "delivered".

| Service | Default address (`HTTP_ADDR`) | Endpoints |
| --- | --- | --- |
| dispatcher | `:8080` | `POST /v1/notifications`, `GET /healthz` |
| webhook | `:8080` (the compose stack publishes it on host port 8081) | `POST /v1/webhooks/{provider}`, `GET /healthz` |

Request and response bodies are JSON in UTF-8, and Arabic text is stored as sent. Every error body is `{"error": "<message>"}`, except a validation failure, which also lists the fields (below). Source of truth: `internal/adapters/httpapi` and `internal/domain/request.go`.

## POST /v1/notifications

Sends one notification to one user over one or more channels. The body names the channels in `content`, and each channel becomes its own row and is delivered independently.

**Authentication:** the header `X-API-Key` must be one of the keys in the dispatcher's `DISPATCHER_API_KEYS` (comma-separated). There are no per-key permissions. Each key has its own rate limit bucket (`RATE_LIMIT_RPS`, default 1000 per second, and `RATE_LIMIT_BURST`, default 2000); the limit is per dispatcher instance.

### Request body

| Field | Type | Required | Rule |
| --- | --- | --- | --- |
| `idempotency_key` | string | yes | At most 255 bytes, no leading or trailing whitespace. The caller's own id for this notification; see below |
| `recipient_id` | string | yes | Not blank. Opaque to Eshaar |
| `notification_type` | string | yes | Not blank, for example `otp` or `order_update` |
| `email` | string | when `content` has `email` | The address for the email channel |
| `phone` | string | when `content` has `sms` or `whatsapp` | The number for those channels |
| `content` | object | yes | At least one channel. Keys are `email`, `sms`, `whatsapp`; any other key is an error |

`content.sms` is `{"text": "<non-blank string>"}`. `content.email` and `content.whatsapp` are `{"template_id": "<non-blank string>", "variables": {...}}`; `variables` is optional and may hold any JSON values. Unknown fields inside a block are rejected, as are unknown fields at the top level. Strings must be valid UTF-8 without NUL characters. The whole body is limited to 512 KiB and must be a single JSON object.

Opt-ins, user preferences and send limits are the caller's responsibility; Eshaar does not check them.

### Responses

| Status | Meaning |
| --- | --- |
| `202` | Kafka confirmed the message. Body: `{"status":"accepted","idempotency_key":"..."}` |
| `400` | Malformed JSON, an empty body, a body that is not a single object, an unknown field, or a validation failure. A validation failure lists every broken rule: `{"error":"invalid request","fields":[{"field":"phone","message":"is required when content has an sms or whatsapp channel"}]}` |
| `401` | `X-API-Key` missing or not a configured key |
| `413` | Body larger than 512 KiB |
| `429` | The key's rate limit is used up. Has a `Retry-After` header (seconds) |
| `503` | Kafka did not confirm within 1 second. Safe to retry with the same `idempotency_key` |

### Idempotency

Retrying with the same `idempotency_key` is always safe. The key is the Kafka message key and, with the channel, the unique key of the stored row, so a second request with the same key produces no second row and no second send per channel, whether it reaches the dispatcher twice or Kafka delivers it twice. A repeat with a different body is still treated as the same notification: the first stored body wins. After a `202` the dispatcher does not tell a caller whether a request was a repeat.

### Example

```
curl -i http://localhost:8080/v1/notifications \
  -H 'X-API-Key: loadtest-1' \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "order-1042-shipped",
    "recipient_id": "user-77",
    "notification_type": "order_update",
    "email": "layla@example.com",
    "phone": "+966500000000",
    "content": {
      "email": {"template_id": "order-shipped", "variables": {"order": "1042"}},
      "sms":   {"text": "تم شحن طلبك رقم 1042"}
    }
  }'
```

```
HTTP/1.1 202 Accepted
Content-Type: application/json; charset=utf-8

{"idempotency_key":"order-1042-shipped","status":"accepted"}
```

This request makes two rows, `email` and `sms`. Delivery is asynchronous: the sender tries the channel's providers in priority order and records `accepted` (a provider took it), `failed` (every provider refused it) or `unresolved` (a provider may have taken it, and nothing says whether it did). A provider's later webhook moves `accepted` to `delivered` or `failed`.

## POST /v1/webhooks/{provider}

Where a provider reports the final state of a message it accepted. `{provider}` is the provider's name from `infra/providers.yaml`, for example `sms-a`.

**Authentication:** the signature, not an API key. `X-Signature` carries the lowercase hex HMAC-SHA256 of the raw request body, keyed with that provider's `webhook_secret` (at least 16 characters of letters, digits and `. _ + -`). There is no rate limit in front of this endpoint. A provider with no configured secret cannot deliver webhooks.

### Request body

One JSON object, at most 64 KiB, valid UTF-8. Field names are matched exactly (case-sensitive). Fields it does not read are ignored and kept in the stored payload.

| Field | Required | Rule |
| --- | --- | --- |
| `message_id` | yes | The id the provider returned when it accepted the notification. At most 255 bytes |
| `channel` | yes | `email`, `sms` or `whatsapp`, and one that this provider serves |
| `status` | yes | `delivered` or `failed`; nothing else is accepted, since only final states are recorded |
| `event_id` | no | The provider's id for this event. With the provider name it makes a resend a duplicate; an event without one is never deduplicated. At most 255 bytes |
| `idempotency_key` | no | Lets an event link to a notification whose accept was never recorded (an `unresolved` row). At most 255 bytes |
| `reason` | no | Free text, kept to 500 characters |

### Responses

| Status | Meaning |
| --- | --- |
| `200` | The event is stored. Also returned for a duplicate, and for an event that cannot be linked to a notification yet (a resolver loop links it when the accept is recorded). Body: `{"status":"ok"}` |
| `400` | The signature is right but the body is not a valid event. The message says which rule |
| `401` | Wrong or missing signature, or an unknown provider; the two answer the same, so the response does not reveal which providers exist |
| `413` | Body larger than 64 KiB |
| `503` | The event could not be stored (a restart or a stalled database). The provider should retry |

The service answers `200` only after the event is in Postgres. The webhook records the final state and never triggers a fallback to another provider.

### Example

```
body='{"event_id":"evt-1","message_id":"sms-a-1f3c9a2b-17","channel":"sms","status":"delivered"}'
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SMS_A_WEBHOOK_SECRET" -hex | sed 's/^.* //')
curl -i http://localhost:8081/v1/webhooks/sms-a \
  -H "X-Signature: $sig" -H 'Content-Type: application/json' -d "$body"
```

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8

{"status":"ok"}
```

## GET /healthz

Served by both services without authentication. Returns `200` and `{"status":"ok"}` while the process is running. It does not check Kafka or Postgres.

## Configuration

Settings come from environment variables only. Required ones have no default and a missing one stops the process at startup with every problem listed.

| Variable | Used by | Default | Meaning |
| --- | --- | --- | --- |
| `DATABASE_URL` | sender, webhook, janitor, migrate | none | Postgres connection string |
| `KAFKA_BROKERS` | dispatcher, sender | none | Comma-separated brokers |
| `KAFKA_TOPIC` | dispatcher, sender | `notifications` | Topic name |
| `HTTP_ADDR` | dispatcher, webhook | `:8080` | Listen address |
| `DISPATCHER_API_KEYS` | dispatcher | none | Comma-separated API keys |
| `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` | dispatcher | `1000`, `2000` | Per-key token bucket |
| `PROVIDERS_CONFIG` | sender, webhook, janitor | `infra/providers.yaml` | Providers per channel with priority, rate, timeout, breaker, webhook secret and mock behavior |
| `LOG_LEVEL` | all | `info` | `log/slog` level |

The sender, webhook and janitor settings (pool size, batch sizes, intervals) are listed in the Configuration section of `docs/decision_log.md`.
