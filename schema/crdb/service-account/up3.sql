CREATE INDEX IF NOT EXISTS service_account_parent
ON omicron.public.service_account (scope, resource_id, id)
WHERE time_deleted IS NULL;
