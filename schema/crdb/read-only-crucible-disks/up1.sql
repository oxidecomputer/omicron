ALTER TABLE
    omicron.public.disk_type_crucible
ADD COLUMN IF NOT EXISTS
    read_only BOOL NOT NULL DEFAULT FALSE;
