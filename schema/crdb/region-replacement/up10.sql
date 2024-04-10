CREATE INDEX IF NOT EXISTS lookup_any_disk_by_volume_id ON omicron.public.disk (
    volume_id
);
