ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS updater_id UUID
        DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS updater_gen INT
        NOT NULL DEFAULT 0;
