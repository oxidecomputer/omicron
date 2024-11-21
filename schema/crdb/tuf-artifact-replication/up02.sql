ALTER TABLE omicron.public.sled
    ADD COLUMN IF NOT EXISTS repo_depot_port INT4
        CHECK (port BETWEEN 0 AND 65535)
        NOT NULL DEFAULT 0;
