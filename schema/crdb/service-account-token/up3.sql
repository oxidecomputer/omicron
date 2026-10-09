CREATE INDEX IF NOT EXISTS service_account_token_by_account
ON omicron.public.service_account_token (service_account_id, id)
WHERE time_deleted IS NULL;
