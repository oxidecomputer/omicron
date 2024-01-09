ALTER TABLE omicron.public.instance
    ADD COLUMN IF NOT EXISTS ssh_keys STRING[] NOT NULL;
