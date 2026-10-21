CREATE UNIQUE INDEX IF NOT EXISTS lookup_router_configuration_by_name ON omicron.public.router_configuration (
    name
) WHERE
    time_deleted IS NULL;
