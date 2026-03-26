CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_subnet_by_subnet
ON omicron.public.external_subnet (subnet)
WHERE
    time_deleted IS NULL;
