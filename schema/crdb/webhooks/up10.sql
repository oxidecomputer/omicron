CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx_subscription (
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- An event class to which the receiver is subscribed.
    event_class omicron.public.webhook_event_class NOT NULL,
    -- If this subscription is a concrete instantiation of a glob pattern, the
    -- value of the glob that created it (and, a foreign key into
    -- `webhook_rx_event_glob`). If the receiver is subscribed to this exact
    -- event class, then this is NULL.
    --
    -- This is used when deleting a glob subscription, as it is necessary to
    -- delete any concrete subscriptions to individual event classes matching
    -- that glob.
    glob STRING(512),

    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (rx_id, event_class)
);
