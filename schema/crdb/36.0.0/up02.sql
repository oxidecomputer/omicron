CREATE UNIQUE INDEX IF NOT EXISTS lookup_resource_by_zpool ON omicron.public.sled_resource (
    zpool_id,
    id
) WHERE zpool_id IS NOT NULL;
