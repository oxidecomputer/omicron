-- Index for removing tokens for a user that's being deleted
CREATE INDEX IF NOT EXISTS lookup_device_access_token_by_silo_user
    ON omicron.public.device_access_token (silo_user_id);
