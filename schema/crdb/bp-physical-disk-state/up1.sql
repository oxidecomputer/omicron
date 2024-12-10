ALTER TABLE omicron.public.bp_omicron_physical_disk
    ADD COLUMN IF NOT EXISTS state omicron.public.physical_disk_state
        NOT NULL 

        -- We should not have any decommissioned disks in an existing blueprint
        DEFAULT 'active';
