ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS reconciler_status_kind inv_config_reconciler_status_kind NOT NULL;
