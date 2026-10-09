CREATE TABLE IF NOT EXISTS omicron.public.service_account (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    scope STRING NOT NULL,
    resource_id UUID NOT NULL,
    federation_generation INT8 NOT NULL DEFAULT 1,
    federation_token_max_ttl_seconds INT8 NOT NULL,
    identity_provider_id UUID,
    trust_policy STRING,
    CONSTRAINT service_account_scope CHECK (scope IN ('silo', 'project')),
    CONSTRAINT service_account_federation CHECK (
        (identity_provider_id IS NULL AND trust_policy IS NULL)
        OR (identity_provider_id IS NOT NULL AND trust_policy IS NOT NULL)
    )
);
