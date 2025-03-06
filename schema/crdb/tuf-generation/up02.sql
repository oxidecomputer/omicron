ALTER TABLE omicron.public.tuf_artifact
    ADD COLUMN IF NOT EXISTS generation_added INT8 NOT NULL DEFAULT 0;
