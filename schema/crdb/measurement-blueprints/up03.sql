ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS measurements omicron.public.bp_sled_measurements NOT NULL DEFAULT 'unknown';
