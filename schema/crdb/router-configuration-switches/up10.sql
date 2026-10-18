ALTER TABLE omicron.public.router_configuration_static_route
    DROP CONSTRAINT IF EXISTS router_configuration_static_route_pkey,
    ADD CONSTRAINT IF NOT EXISTS router_configuration_static_route_pkey_switch
        PRIMARY KEY (router_configuration_id, switch, name);
