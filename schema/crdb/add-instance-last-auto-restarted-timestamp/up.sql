ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS time_last_auto_restarted TIMESTAMPTZ;
