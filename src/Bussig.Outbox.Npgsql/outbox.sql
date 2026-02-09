CREATE TABLE IF NOT EXISTS "{0}".outbox_messages (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id          UUID NOT NULL,
    queue_name          TEXT NOT NULL,
    body                BYTEA NOT NULL,
    headers_json        TEXT,
    priority            SMALLINT,
    delay               INTERVAL,
    message_version     INTEGER NOT NULL DEFAULT 1,
    expiration_time     TIMESTAMPTZ,
    scheduling_token_id UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    published_at        TIMESTAMPTZ
);

-- Efficient polling for unpublished messages
CREATE INDEX IF NOT EXISTS ix_outbox_messages_pending
    ON "{0}".outbox_messages (id) WHERE published_at IS NULL;

-- Efficient cancel by scheduling token
CREATE INDEX IF NOT EXISTS ix_outbox_messages_scheduling_token
    ON "{0}".outbox_messages (scheduling_token_id)
    WHERE scheduling_token_id IS NOT NULL AND published_at IS NULL;
