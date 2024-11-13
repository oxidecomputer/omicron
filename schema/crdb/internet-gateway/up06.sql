CREATE UNIQUE INDEX IF NOT EXISTS lookup_internet_gateway_ip_address_by_igw_id ON omicron.public.internet_gateway_ip_address (
    internet_gateway_id
) WHERE
    time_deleted IS NULL;
