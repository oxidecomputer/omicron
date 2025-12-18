ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS tuf_repo_pruner_enabled BOOL NOT NULL DEFAULT TRUE;
