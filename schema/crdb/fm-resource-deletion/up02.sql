ALTER TABLE omicron.public.support_bundle
    ADD COLUMN IF NOT EXISTS time_deleted TIMESTAMPTZ;
