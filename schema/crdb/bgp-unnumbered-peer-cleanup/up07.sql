ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config_communities
    DROP CONSTRAINT IF EXISTS switch_port_settings_bgp_peer_config_communities_pkey,
    ADD CONSTRAINT switch_port_settings_bgp_peer_config_communities_pkey
        PRIMARY KEY (id);
