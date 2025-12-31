CREATE TABLE IF NOT EXISTS omicron.public.attached_external_subnet (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    external_subnet_id UUID NOT NULL,
    subnet INET NOT NULL,
    instance_id UUID NOT NULL
);
