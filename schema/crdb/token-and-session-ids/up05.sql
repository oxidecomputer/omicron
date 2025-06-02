CREATE UNIQUE INDEX IF NOT EXISTS console_session_token_unique
  ON omicron.public.console_session (token);
