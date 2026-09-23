CREATE UNIQUE INDEX IF NOT EXISTS federation_session_token
ON omicron.public.federation_session (token);
