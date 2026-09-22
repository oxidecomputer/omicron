CREATE TABLE IF NOT EXISTS omicron.public.federation_role_grant (
    id UUID PRIMARY KEY,
    resource_kind STRING NOT NULL,
    resource_id UUID NOT NULL,
    role_name STRING NOT NULL,
    trust_policy_id UUID NOT NULL
);
