CREATE TABLE processed_messages (
    message_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX processed_messages_received_at_idx
    ON processed_messages (received_at DESC, message_id DESC);

CREATE TABLE event_identities (
    message_id UUID PRIMARY KEY,
    fingerprint BYTEA NOT NULL
);
