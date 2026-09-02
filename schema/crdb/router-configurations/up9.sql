CREATE UNIQUE INDEX IF NOT EXISTS router_configuration_bgp_peer_unnumbered_unique
    ON omicron.public.router_configuration_bgp_peer (router_configuration_id, port_name)
    WHERE addr IS NULL;
