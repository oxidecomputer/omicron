CREATE UNIQUE INDEX IF NOT EXISTS router_configuration_bgp_peer_numbered_unique
    ON omicron.public.router_configuration_bgp_peer (router_configuration_id, addr)
    WHERE addr IS NOT NULL;
