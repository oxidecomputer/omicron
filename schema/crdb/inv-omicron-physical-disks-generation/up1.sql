ALTER TABLE omicron.public.inv_sled_agent
    ADD COLUMN IF NOT EXISTS omicrion_physical_disks_generation INT8
        NOT NULL DEFAULT 0;
