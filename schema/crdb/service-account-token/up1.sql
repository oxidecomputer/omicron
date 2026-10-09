CREATE TABLE IF NOT EXISTS omicron.public.service_account_token (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    service_account_id UUID NOT NULL,
    token STRING(40) NOT NULL,
    idp_id UUID,
    federation_jwt_claims JSONB,
    federation_generation INT8,
    time_expires TIMESTAMPTZ,
    time_deleted TIMESTAMPTZ,
    CONSTRAINT service_account_token_federation CHECK (
        (idp_id IS NULL AND federation_jwt_claims IS NULL
            AND federation_generation IS NULL)
        OR (idp_id IS NOT NULL AND federation_jwt_claims IS NOT NULL
            AND federation_generation IS NOT NULL AND time_expires IS NOT NULL)
    )
);
