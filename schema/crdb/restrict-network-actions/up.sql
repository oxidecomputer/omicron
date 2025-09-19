ALTER TABLE omicron.public.silo
    ADD COLUMN restrict_network_actions BOOL
        NOT NULL
        DEFAULT FALSE;
