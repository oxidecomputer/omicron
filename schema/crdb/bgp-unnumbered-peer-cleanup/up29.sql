CREATE UNIQUE INDEX IF NOT EXISTS
    switch_port_settings_bgp_peer_config_allow_export_unnumbered_unique
    ON omicron.public.switch_port_settings_bgp_peer_config_allow_export
        (port_settings_id, interface_name, prefix)
    WHERE addr IS NULL;
