CREATE UNIQUE INDEX IF NOT EXISTS lookup_unadopted_rack_by_id ON omicron.public.unadopted_rack (
    id
) WHERE time_deleted IS NULL;
