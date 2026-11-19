CREATE TABLE IF NOT EXISTS omicron.public.service_account (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    silo_id UUID NOT NULL,
    project_id UUID NOT NULL
);
