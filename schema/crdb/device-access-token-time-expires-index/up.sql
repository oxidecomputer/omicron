CREATE INDEX IF NOT EXISTS lookup_device_access_token_by_expiration
    ON omicron.public.device_access_token (time_expires);
