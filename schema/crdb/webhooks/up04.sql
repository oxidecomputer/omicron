CREATE TABLE IF NOT EXISTS omicron.public.webhook_secret (
    -- ID of this secret.
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    -- N.B. that this will always be equal to `time_created` for secrets, as
    -- they are never modified once created.
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- Secret value.
    secret STRING(512) NOT NULL
);
