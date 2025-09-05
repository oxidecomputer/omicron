ALTER TABLE omicron.public.silo_quotas
    ADD CONSTRAINT IF NOT EXISTS cpus_not_negative CHECK (cpus >= 0),
    ADD CONSTRAINT IF NOT EXISTS memory_not_negative CHECK (memory_bytes >= 0),
    ADD CONSTRAINT IF NOT EXISTS storage_not_negative CHECK (storage_bytes >= 0);
