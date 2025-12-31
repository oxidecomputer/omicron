CREATE UNIQUE INDEX IF NOT EXISTS lookup_external_subnet_by_subnet
ON omicron.public.subnet_pool_member (subnet)
WHERE
    time_deleted IS NULL;
