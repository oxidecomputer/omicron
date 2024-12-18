CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx_subscription (
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- An event class to which this receiver is subscribed.
    event_class STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (rx_id, event_class)
);
