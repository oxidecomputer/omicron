CREATE UNIQUE INDEX IF NOT EXISTS lookup_internet_gateway_by_vpc ON omicron.public.internet_gateway (
    vpc_id,
    name
) WHERE
    time_deleted IS NULL;
