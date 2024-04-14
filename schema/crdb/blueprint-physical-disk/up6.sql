CREATE UNIQUE INDEX IF NOT EXISTS lookup_physical_disk_by_sled ON omicron.public.physical_disk (
    sled_id,
    id
);
