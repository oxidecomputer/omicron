CREATE UNIQUE INDEX IF NOT EXISTS
    lookup_rack_by_subnet
ON omicron.public.rack (
    rack_subnet
)
WHERE rack_subnet IS NOT NULL;
