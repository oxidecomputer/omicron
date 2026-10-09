CREATE UNIQUE INDEX IF NOT EXISTS service_account_grant_unique
ON omicron.public.service_account_grant
    (service_account_id, resource_kind, resource_id, role_name);
