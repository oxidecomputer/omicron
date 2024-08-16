CREATE TABLE IF NOT EXISTS omicron.public.bp_target (
    version INT8 PRIMARY KEY,
    blueprint_id UUID NOT NULL,
    enabled BOOL NOT NULL,
    time_made_target TIMESTAMPTZ NOT NULL
);
