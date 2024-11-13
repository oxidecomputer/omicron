CREATE INDEX IF NOT EXISTS lookup_routers_in_vpc ON omicron.public.vpc_router (
    vpc_id
) WHERE
    time_deleted IS NULL;
