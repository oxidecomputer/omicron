ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS reconciler_status_duration_secs FLOAT;
