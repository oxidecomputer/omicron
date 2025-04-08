ALTER TABLE omicron.public.physical_disk
    ADD COLUMN IF NOT EXISTS disk_policy omicron.public.physical_disk_policy
        NOT NULL DEFAULT 'in_service',
    ADD COLUMN IF NOT EXISTS disk_state omicron.public.physical_disk_state
        NOT NULL DEFAULT 'active';
