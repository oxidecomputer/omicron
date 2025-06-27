ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS remove_mupdate_override UUID DEFAULT NULL;
