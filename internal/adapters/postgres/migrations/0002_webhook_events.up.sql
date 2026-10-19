-- Provider callbacks, stored as received. Event ids are scoped by provider,
-- so the unique key is (provider, event_id). event_id is nullable: Postgres
-- treats NULLs as distinct, so events without an id never collide here.
CREATE TABLE webhook_events (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    provider            text        NOT NULL,
    channel             text        NOT NULL,
    event_id            text,
    provider_message_id text        NOT NULL,
    notification_id     uuid        REFERENCES notifications (id),
    payload             jsonb       NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT webhook_events_provider_event_key UNIQUE (provider, event_id),
    CONSTRAINT webhook_events_channel_check CHECK (channel IN ('email', 'sms', 'whatsapp'))
);

-- Resolving an event to its notification, and finding the events for a
-- message once the notification is accepted.
CREATE INDEX webhook_events_provider_message_id_idx
    ON webhook_events (provider, provider_message_id);

-- Events of one notification, and the FK check when a notification is deleted.
CREATE INDEX webhook_events_notification_id_idx
    ON webhook_events (notification_id)
    WHERE notification_id IS NOT NULL;

-- Events that arrived before the notification was accepted and still need
-- resolving, oldest first.
CREATE INDEX webhook_events_unresolved_idx
    ON webhook_events (created_at)
    WHERE notification_id IS NULL;
