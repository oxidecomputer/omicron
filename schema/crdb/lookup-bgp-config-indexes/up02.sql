CREATE INDEX IF NOT EXISTS lookup_sps_bgp_peer_config_by_bgp_config_id on omicron.public.switch_port_settings_bgp_peer_config(
    bgp_config_id
);
