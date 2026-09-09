ALTER TABLE omicron.public.reconfigurator_config
    ADD COLUMN IF NOT EXISTS blueprint_pruner_nkeep INT8 NOT NULL DEFAULT 1000;
