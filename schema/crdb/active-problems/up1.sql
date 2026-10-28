CREATE TABLE IF NOT EXISTS omicron.public.active_problem (
    id UUID PRIMARY KEY,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    current_revision INT8 NOT NULL,

    CONSTRAINT revision_is_non_negative CHECK (current_revision >= 0)
);
