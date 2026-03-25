ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config_allow_export
    DROP CONSTRAINT IF EXISTS switch_port_settings_bgp_peer_config_allow_export_pkey,
    ADD CONSTRAINT switch_port_settings_bgp_peer_config_allow_export_pkey
        PRIMARY KEY (id);
