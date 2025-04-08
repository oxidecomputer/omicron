-- Sled Agent upserts are now conditional on a generation number
ALTER TABLE omicron.public.sled
ADD COLUMN IF NOT EXISTS sled_agent_gen INT8 NOT NULL
    DEFAULT 1;
