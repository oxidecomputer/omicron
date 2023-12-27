ALTER TABLE omicron.public.physical_disk ADD COLUMN IF NOT EXISTS state omicron.public.physical_disk_state NOT NULL DEFAULT 'active';
