ALTER TABLE omicron.public.physical_disk
    ADD COLUMN IF NOT EXISTS disk_policy omicron.public.disk_policy
        NOT NULL DEFAUlT 'in_service',
    ADD COLUMN IF NOT EXISTS disk_state omicron.public.disk_state
        NOT NULL DEFAULT 'active';
