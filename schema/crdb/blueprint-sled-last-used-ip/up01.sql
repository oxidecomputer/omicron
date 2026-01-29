ALTER TABLE omicron.public.bp_sled_metadata
    ADD COLUMN IF NOT EXISTS last_allocated_ip_subnet_offset INT4
    CHECK (last_allocated_ip_subnet_offset BETWEEN 0 AND 65535);
