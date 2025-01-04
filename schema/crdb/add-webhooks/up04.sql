CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx_subscription (
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- An event class (or event class glob) to which this receiver is subscribed.
    event_class STRING(512) NOT NULL,
    -- The event class or event classs glob transformed into a patteern for use
    -- in SQL `SIMILAR TO` clauses.
    --
    -- This is a bit interesting: users specify event class globs as sequences
    -- of dot-separated segments which may be `*` to match any one segment or
    -- `**` to match any number of segments. In order to match webhook events to
    -- subscriptions within the database, we transform these into patterns that
    -- can be used with a `SIMILAR TO` clause.
    similar_to STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (rx_id, event_class)
);
