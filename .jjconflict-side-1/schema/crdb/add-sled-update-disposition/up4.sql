ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS update_availability
        omicron.public.sled_update_availability
        NOT NULL DEFAULT 'available';
