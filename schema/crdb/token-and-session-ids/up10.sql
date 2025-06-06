CREATE UNIQUE INDEX IF NOT EXISTS device_access_token_unique
  ON omicron.public.device_access_token (token);
