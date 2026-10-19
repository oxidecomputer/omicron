ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS multirack_join_in_progress BOOLEAN NOT NULL DEFAULT false;
