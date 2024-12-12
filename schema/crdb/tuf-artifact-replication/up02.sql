ALTER TABLE omicron.public.sled
    ADD COLUMN IF NOT EXISTS repo_depot_port INT4
        CHECK (port BETWEEN 0 AND 65535)
        -- This is the value of the `REPO_DEPOT_PORT` const. If we're running
        -- this migration, we're running on a "real" system and this is
        -- definitely the correct port. (The DEFAULT value is removed in up03.)
        NOT NULL DEFAULT 12348;
