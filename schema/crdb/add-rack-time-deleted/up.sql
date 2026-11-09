ALTER TABLE omicron.public.rack
    ADD COLUMN IF NOT EXISTS time_deleted TIMESTAMPTZ;
