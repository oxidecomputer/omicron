CREATE UNIQUE INDEX IF NOT EXISTS router_configuration_bgp_peer_numbered_unique
    ON omicron.public.router_configuration_bgp_peer (router_configuration_id, switch, addr)
    WHERE addr IS NOT NULL;
