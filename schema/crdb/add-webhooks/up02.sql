CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx_secret (
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- ID of this secret.
    signature_id STRING(63) NOT NULL,
    -- Secret value.
    secret BYTES NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    PRIMARY KEY (signature_id, rx_id)
);
