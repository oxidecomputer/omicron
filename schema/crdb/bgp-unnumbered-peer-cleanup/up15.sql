ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config_allow_import
    DROP CONSTRAINT IF EXISTS switch_port_settings_bgp_peer_config_allow_import_pkey,
    ADD CONSTRAINT switch_port_settings_bgp_peer_config_allow_import_pkey
        PRIMARY KEY (id);
