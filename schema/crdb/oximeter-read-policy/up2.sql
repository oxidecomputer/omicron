CREATE TABLE IF NOT EXISTS omicron.public.oximeter_read_policy (
    version INT8 PRIMARY KEY,
    oximeter_read_mode omicron.public.oximeter_read_mode NOT NULL,
    time_created TIMESTAMPTZ NOT NULL
);
