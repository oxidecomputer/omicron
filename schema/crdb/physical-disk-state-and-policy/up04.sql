ALTER TABLE omicron.public.physical_disk
    ALTER COLUMN disk_policy DROP DEFAULT,
    ALTER COLUMN disk_state DROP DEFAULT;
