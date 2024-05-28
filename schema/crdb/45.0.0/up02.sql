CREATE TABLE IF NOT EXISTS omicron.public.bootstore_config (
    key TEXT NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (key, generation),
    data JSONB NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);
