CREATE INDEX IF NOT EXISTS lookup_zpool_by_disk ON omicron.public.zpool (physical_disk_id, id) WHERE physical_disk_id IS NOT NULL AND time_deleted IS NULL;
