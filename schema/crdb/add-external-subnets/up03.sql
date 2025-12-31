CREATE INDEX ON omicron.public.external_subnet
lookup_external_subnet_by_subnet_pool_id (external_subnet_pool_id)
WHERE
    time_deleted IS NOT NULL;
