CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_pool_member_by_first_and_last_address
ON omicron.public.subnet_pool_member (first_address, last_address)
WHERE
    time_deleted IS NULL;
