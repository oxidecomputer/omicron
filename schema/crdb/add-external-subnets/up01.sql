CREATE TABLE IF NOT EXISTS omicron.public.subnet_pool (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    rcgen INT8 NOT NULL,
    ip_version omicron.public.ip_version NOT NULL
);
