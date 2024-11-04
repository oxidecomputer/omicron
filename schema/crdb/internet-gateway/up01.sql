CREATE TABLE IF NOT EXISTS omicron.public.internet_gateway (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    vpc_id UUID NOT NULL,
    rcgen INT NOT NULL,
    resolved_version INT NOT NULL DEFAULT 0
);
