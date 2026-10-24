ALTER TABLE event_identities
    ADD COLUMN registered_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX event_identities_registered_at_idx
    ON event_identities (registered_at);
