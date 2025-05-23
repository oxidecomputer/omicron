ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS ledgered_sled_config UUID;
