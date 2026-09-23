CREATE TABLE IF NOT EXISTS omicron.public.federation_session (
    id UUID PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    time_expires TIMESTAMPTZ NOT NULL,
    trust_policy_id UUID NOT NULL,
    trust_policy_revision INT8 NOT NULL,
    jwt_claims JSONB NOT NULL,
    audit_log_id UUID NOT NULL,
    token STRING(40) NOT NULL
);
