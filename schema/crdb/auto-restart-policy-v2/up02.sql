ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS old_auto_restart_policy omicron.public.instance_auto_restart;
