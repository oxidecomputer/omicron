CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_pool_member_by_subnet
ON omicron.public.subnet_pool_member (subnet)
WHERE
    time_deleted IS NULL;
