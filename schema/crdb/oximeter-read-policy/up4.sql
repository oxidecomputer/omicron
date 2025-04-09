CREATE TABLE IF NOT EXISTS omicron.public.bp_oximeter_read_policy (
    blueprint_id UUID PRIMARY KEY,
    -- TODO-K: REMOVE THIS? I DON'T THINK IT'S NECESSARY.
    version INT8 NOT NULL,
    oximeter_read_mode omicron.public.oximeter_read_mode NOT NULL
);