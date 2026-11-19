CREATE UNIQUE INDEX IF NOT EXISTS lookup_service_account_key_by_service_account
    ON omicron.public.service_account_key (service_account_id, name)
    WHERE time_deleted IS NULL;
