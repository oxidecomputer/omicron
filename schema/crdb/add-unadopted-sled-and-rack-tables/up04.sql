CREATE UNIQUE INDEX IF NOT EXISTS lookup_unadopted_sled_by_id ON omicron.public.unadopted_sled (
    id
) WHERE time_deleted IS NULL;
