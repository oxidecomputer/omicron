CREATE TABLE IF NOT EXISTS omicron.public.alert_subscription (
    -- UUID of the alert receiver (foreign key into
    -- `omicron.public.alert_receiver`)
    rx_id UUID NOT NULL,
    -- An alert class to which the receiver is subscribed.
    alert_class omicron.public.alert_class NOT NULL,
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

    PRIMARY KEY (rx_id, alert_class)
);
