CREATE UNIQUE INDEX IF NOT EXISTS
    switch_port_settings_bgp_peer_config_allow_import_numbered_unique
    ON omicron.public.switch_port_settings_bgp_peer_config_allow_import
        (port_settings_id, interface_name, addr, prefix)
    WHERE addr IS NOT NULL;
