ALTER TABLE omicron.public.sled
    ADD COLUMN IF NOT EXISTS provision_state omicron.public.sled_provision_state
    NOT NULL DEFAULT 'provisionable';
