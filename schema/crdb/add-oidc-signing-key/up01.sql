CREATE TABLE IF NOT EXISTS omicron.public.oidc_signing_key (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    kid STRING(255) NOT NULL,
    algorithm STRING(16) NOT NULL,
    public_key BYTES NOT NULL,
    private_key BYTES NOT NULL
);
