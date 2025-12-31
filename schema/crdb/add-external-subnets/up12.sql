CREATE INDEX IF NOT EXISTS lookup_external_subnet_by_subnet_pool_member_id
ON omicron.public.external_subnet (subnet_pool_member_id)
WHERE
    time_deleted IS NOT NULL;
