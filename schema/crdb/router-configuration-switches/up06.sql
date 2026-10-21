ALTER TABLE omicron.public.router_configuration_bgp_peer
    DROP CONSTRAINT IF EXISTS router_configuration_bgp_peer_pkey,
    ADD CONSTRAINT IF NOT EXISTS router_configuration_bgp_peer_pkey_switch
        PRIMARY KEY (router_configuration_id, switch, name);
