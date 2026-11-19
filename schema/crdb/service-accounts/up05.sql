CREATE UNIQUE INDEX IF NOT EXISTS service_account_key_public_key_unique
    ON omicron.public.service_account_key (public_key)
    WHERE time_deleted IS NULL;
