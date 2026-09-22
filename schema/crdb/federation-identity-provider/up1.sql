CREATE TABLE IF NOT EXISTS omicron.public.federation_identity_provider (
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    silo_id UUID NOT NULL,
    audience STRING NOT NULL,
    issuer STRING NOT NULL,
    verification_type STRING NOT NULL,
    discovery_url STRING,
    signing_keys JSONB,
    CONSTRAINT verification_configuration CHECK (
        (verification_type = 'oidc_discovery'
            AND discovery_url IS NOT NULL AND signing_keys IS NULL)
        OR (verification_type = 'static_jwks'
            AND discovery_url IS NULL AND signing_keys IS NOT NULL)
    )
);
