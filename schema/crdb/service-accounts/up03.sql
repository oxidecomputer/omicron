CREATE TABLE IF NOT EXISTS omicron.public.service_account_key (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    time_expires TIMESTAMPTZ,
    service_account_id UUID NOT NULL,
    public_key BYTES NOT NULL
);
