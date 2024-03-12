CREATE TABLE IF NOT EXISTS omicron.public.probe (
    id UUID NOT NULL PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    project_id UUID NOT NULL,
    sled UUID NOT NULL
);
