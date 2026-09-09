ALTER TABLE omicron.public.instance
  ADD COLUMN IF NOT EXISTS shutdown_policy_timeout INTERVAL DEFAULT NULL;
