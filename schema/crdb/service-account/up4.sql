CREATE TABLE IF NOT EXISTS omicron.public.service_account_grant (
    id UUID PRIMARY KEY,
    service_account_id UUID NOT NULL,
    resource_kind STRING NOT NULL,
    resource_id UUID NOT NULL,
    role_name STRING NOT NULL,
    CONSTRAINT service_account_grant_kind CHECK (
        resource_kind IN ('silo', 'project')
    )
);
