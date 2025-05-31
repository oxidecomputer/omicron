CREATE TABLE IF NOT EXISTS omicron.public.silo_settings (
    silo_id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,

    -- null means no max: users can tokens that never expire
    device_token_max_ttl_seconds INT8 CHECK (device_token_max_ttl_seconds > 0)
);
