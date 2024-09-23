ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS auto_restart_policy omicron.public.instance_auto_restart;
