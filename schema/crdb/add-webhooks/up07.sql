CREATE TABLE IF NOT EXISTS omicron.public.webhook_msg (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    -- Set when dispatch entries have been created for this event.
    time_dispatched TIMESTAMPTZ,
    -- The class of event that this is.
    event_class STRING(512) NOT NULL,
    -- Actual event data. The structure of this depends on the event class.
    event JSONB NOT NULL
);
