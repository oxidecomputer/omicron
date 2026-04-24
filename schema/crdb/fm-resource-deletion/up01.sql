ALTER TABLE omicron.public.alert
    ADD COLUMN IF NOT EXISTS time_deleted TIMESTAMPTZ;
