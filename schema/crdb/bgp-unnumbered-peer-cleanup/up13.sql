CREATE UNIQUE INDEX IF NOT EXISTS
    switch_port_settings_bgp_peer_config_communities_unnumbered_unique
    ON omicron.public.switch_port_settings_bgp_peer_config_communities
        (port_settings_id, interface_name, community)
    WHERE addr IS NULL;
