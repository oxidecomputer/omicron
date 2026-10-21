CREATE UNIQUE INDEX IF NOT EXISTS lookup_silo_router_configuration_by_router_configuration
    ON omicron.public.silo_router_configuration (router_configuration_id, silo_id);
