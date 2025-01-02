CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery (
    -- UUID of this dispatch.
    id UUID PRIMARY KEY,
    --- UUID of the event (foreign key into `omicron.public.webhook_event`).
    event_id UUID NOT NULL,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    payload JSONB NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    -- If this is set, then this webhook message has either been delivered
    -- successfully, or is considered permanently failed.
    time_completed TIMESTAMPTZ,
);
