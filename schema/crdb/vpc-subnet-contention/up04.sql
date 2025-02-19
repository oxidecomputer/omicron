-- Enforce uniqueness of 'vpc_subnet' routes on parent (and help add/delete).
CREATE UNIQUE INDEX IF NOT EXISTS lookup_subnet_route_by_id ON omicron.public.router_route (
    vpc_subnet_id
) WHERE
    time_deleted IS NULL AND kind = 'vpc_subnet';
