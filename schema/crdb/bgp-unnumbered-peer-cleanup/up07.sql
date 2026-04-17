ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config_communities
    DROP CONSTRAINT IF EXISTS switch_port_settings_bgp_peer_config_communities_pkey,
    ADD CONSTRAINT IF NOT EXISTS switch_port_settings_bgp_peer_config_communities_pkey_id
        PRIMARY KEY (id);
