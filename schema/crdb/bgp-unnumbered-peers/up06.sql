-- Add an index for looking up BGP peers by port_settings_id.
-- This is needed because the partial indexes (for numbered and unnumbered peers)
-- don't cover all rows when filtering only by port_settings_id.
CREATE INDEX IF NOT EXISTS lookup_sps_bgp_peer_config_by_port_settings_id
    ON omicron.public.switch_port_settings_bgp_peer_config (port_settings_id);
