CREATE UNIQUE INDEX IF NOT EXISTS lookup_internet_gateway_ip_pool_by_igw_id ON omicron.public.internet_gateway_ip_pool (
    internet_gateway_id
) WHERE
    time_deleted IS NULL;
