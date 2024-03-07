CREATE TABLE IF NOT EXISTS omicron.public.bootstore_config (
    version INT8 NOT NULL PRIMARY KEY,
    config JSONB NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);
