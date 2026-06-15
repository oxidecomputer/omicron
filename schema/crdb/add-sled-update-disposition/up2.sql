ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS update_disposition_generation INT8
        NOT NULL DEFAULT 1;
