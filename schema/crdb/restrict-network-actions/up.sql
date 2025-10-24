ALTER TABLE omicron.public.silo
    ADD COLUMN IF NOT EXISTS restrict_network_actions BOOL
        NOT NULL
        DEFAULT FALSE;
