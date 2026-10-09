CREATE UNIQUE INDEX IF NOT EXISTS service_account_token_unique
ON omicron.public.service_account_token (token);
