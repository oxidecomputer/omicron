ALTER TABLE omicron.public.device_auth_request
  ADD COLUMN IF NOT EXISTS requested_ttl_seconds INT8 CHECK (requested_ttl_seconds > 0);