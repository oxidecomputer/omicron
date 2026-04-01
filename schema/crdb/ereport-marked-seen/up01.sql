ALTER TABLE IF EXISTS omicron.public.ereport
    ADD COLUMN IF NOT EXISTS marked_seen_in UUID;
