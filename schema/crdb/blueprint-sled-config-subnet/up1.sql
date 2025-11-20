ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS subnet INET;
