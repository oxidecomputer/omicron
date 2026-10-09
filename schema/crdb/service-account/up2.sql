CREATE UNIQUE INDEX IF NOT EXISTS service_account_name
ON omicron.public.service_account (scope, resource_id, name)
WHERE time_deleted IS NULL;
