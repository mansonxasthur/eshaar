-- One row per channel of a request. The unique constraint on
-- (idempotency_key, channel) is what makes Kafka redeliveries and janitor
-- re-drives safe: a second insert is rejected and treated as already saved.
CREATE TABLE notifications (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key     text        NOT NULL,
    recipient_id        text        NOT NULL,
    notification_type   text        NOT NULL,
    channel             text        NOT NULL,
    content             jsonb       NOT NULL,
    email               text,
    phone               text,
    state               text        NOT NULL DEFAULT 'pending',
    failure_reason      text,
    provider            text,           -- provider that accepted the send
    provider_message_id text,
    accepted_at         timestamptz,
    claimed_at          timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    -- updated_at has no trigger: every UPDATE must set it explicitly.
    updated_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT notifications_idempotency_channel_key UNIQUE (idempotency_key, channel),
    CONSTRAINT notifications_channel_check CHECK (channel IN ('email', 'sms', 'whatsapp')),
    CONSTRAINT notifications_state_check CHECK (state IN ('pending', 'accepted', 'delivered', 'failed', 'unresolved')),
    -- An accepted row must be findable by webhooks.
    CONSTRAINT notifications_accepted_check CHECK (
        state <> 'accepted' OR (provider IS NOT NULL AND provider_message_id IS NOT NULL AND accepted_at IS NOT NULL)
    ),
    -- Email rows carry an address, SMS and WhatsApp rows a phone number.
    CONSTRAINT notifications_contact_check CHECK (
        (channel = 'email' AND email IS NOT NULL AND phone IS NULL)
        OR (channel IN ('sms', 'whatsapp') AND phone IS NOT NULL AND email IS NULL)
    )
);

-- Recovery janitor: pending rows nobody has claimed yet, oldest first.
--   WHERE state = 'pending' AND claimed_at IS NULL AND created_at < now() - 30s
-- Partial, so it only holds the small set of rows still waiting.
CREATE INDEX notifications_unclaimed_pending_idx
    ON notifications (created_at)
    WHERE state = 'pending' AND claimed_at IS NULL;

-- Sweeper: pending rows that were claimed and never finished.
--   WHERE state = 'pending' AND claimed_at < now() - 10s
CREATE INDEX notifications_claimed_pending_idx
    ON notifications (claimed_at)
    WHERE state = 'pending' AND claimed_at IS NOT NULL;

-- Webhook lookup by provider and the provider's message id. Message ids are
-- only unique per provider, so the provider is part of the key. Rows that no
-- provider has accepted yet stay out of the index.
CREATE UNIQUE INDEX notifications_provider_message_id_idx
    ON notifications (provider, provider_message_id)
    WHERE provider_message_id IS NOT NULL;
