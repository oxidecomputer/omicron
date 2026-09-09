ALTER TABLE omicron.public.instance
  ADD COLUMN IF NOT EXISTS shutdown_policy_action omicron.public.instance_shutdown_action DEFAULT NULL;
