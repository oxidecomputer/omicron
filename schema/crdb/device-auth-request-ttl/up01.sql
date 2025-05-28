ALTER TABLE omicron.public.device_auth_request
  ADD COLUMN IF NOT EXISTS token_ttl_seconds INT8 CHECK (token_ttl_seconds > 0);
