-- This UNIQUE constraint is critical for ensuring that at most
-- one token is ever created for a given device authorization flow.
CREATE UNIQUE INDEX IF NOT EXISTS lookup_device_access_token_by_client
    ON omicron.public.device_access_token (client_id, device_code);
