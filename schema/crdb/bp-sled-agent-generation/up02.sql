ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS sled_agent_generation INT8;
