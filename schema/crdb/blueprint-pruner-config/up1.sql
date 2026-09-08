ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS blueprint_pruner_enabled BOOL NOT NULL DEFAULT TRUE;
