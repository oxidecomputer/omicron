CREATE UNIQUE INDEX IF NOT EXISTS lookup_disk_by_volume_id ON omicron.public.disk_type_crucible (
    volume_id
);
