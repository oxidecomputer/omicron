CREATE TABLE IF NOT EXISTS omicron.public.webhook_event (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    -- The class of event that this is.
    event_class omicron.public.webhook_event_class NOT NULL,
    -- Actual event data. The structure of this depends on the event class.
    event JSONB NOT NULL,

    -- Set when dispatch entries have been created for this event.
    time_dispatched TIMESTAMPTZ,
    -- The number of receivers that this event was dispatched to.
    num_dispatched INT8 NOT NULL,

    CONSTRAINT time_dispatched_set_if_dispatched CHECK (
        (num_dispatched = 0) OR (time_dispatched IS NOT NULL)
    ),

    CONSTRAINT num_dispatched_is_positive CHECK (
        (num_dispatched >= 0)
    )
);
