ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS omicron_physical_disks_generation INT8
        NOT NULL DEFAULT 1;
