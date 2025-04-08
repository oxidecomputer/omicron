set local disallow_full_table_scans = off;

UPDATE omicron.public.switch_port_settings_bgp_peer_config
    SET enforce_first_as = false
    WHERE enforce_first_as IS NULL;
