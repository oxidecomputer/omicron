ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS last_reconciliation_sled_config UUID;
