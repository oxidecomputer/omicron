ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS auto_restart_cooldown INTERVAL;
