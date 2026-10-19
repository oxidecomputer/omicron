ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS running_multirack_join_service BOOLEAN NOT NULL DEFAULT false;
