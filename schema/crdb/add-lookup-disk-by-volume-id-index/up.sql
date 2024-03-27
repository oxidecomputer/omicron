CREATE UNIQUE INDEX IF NOT EXISTS lookup_disk_by_volume_id ON omicron.public.disk (
    volume_id
) WHERE
    time_deleted IS NULL;
