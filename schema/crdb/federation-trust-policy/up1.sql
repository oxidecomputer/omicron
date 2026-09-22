CREATE TABLE IF NOT EXISTS omicron.public.federation_trust_policy (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    silo_id UUID NOT NULL,
    revision INT8 NOT NULL DEFAULT 1,
    idp_id UUID NOT NULL,
    policy TEXT NOT NULL
);
