ALTER TABLE omicron.public.instance
ADD COLUMN IF NOT EXISTS auto_restart_policy omicron.public.auto_restart_policy;
