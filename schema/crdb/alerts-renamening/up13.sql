CREATE TABLE IF NOT EXISTS omicron.public.alert (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    alert_class omicron.public.alert_class NOT NULL,
    -- Actual alert data. The structure of this depends on the alert class.
    payload JSONB NOT NULL,

    -- Set when dispatch entries have been created for this alert.
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
