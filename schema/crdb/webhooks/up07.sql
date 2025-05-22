CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx_event_glob (
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,
    -- An event class glob to which this receiver is subscribed.
    glob STRING(512) NOT NULL,
    -- Regex used when evaluating this filter against concrete event classes.
    regex STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    -- The database schema version at which this glob was last expanded.
    --
    -- This is used to detect when a glob must be re-processed to generate exact
    -- subscriptions on schema changes.
    --
    -- If this is NULL, no exact subscriptions have been generated for this glob
    -- yet (i.e. it was just created)
    schema_version STRING(64),

    PRIMARY KEY (rx_id, glob)
);
