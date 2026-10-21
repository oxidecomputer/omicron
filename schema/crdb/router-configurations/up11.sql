CREATE UNIQUE INDEX IF NOT EXISTS lookup_control_plane_router_configuration_by_router_configuration
    ON omicron.public.control_plane_router_configuration (router_configuration_id);
