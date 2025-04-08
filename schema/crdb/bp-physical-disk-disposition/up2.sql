ALTER TABLE omicron.public.bp_omicron_physical_disk
    ADD COLUMN IF NOT EXISTS disposition omicron.public.bp_physical_disk_disposition
        NOT NULL 

        -- We should not have any expunged disks in an existing blueprint, as
        -- we only include ones that are `InService`.
        DEFAULT 'in_service';
