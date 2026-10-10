ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS rack_id UUID;
