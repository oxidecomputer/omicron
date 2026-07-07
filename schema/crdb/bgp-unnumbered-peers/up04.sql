-- Add unique constraint for numbered peers (those with addr).
-- This ensures we don't have duplicate peers on the same interface with the same address.
CREATE UNIQUE INDEX IF NOT EXISTS switch_port_settings_bgp_peer_config_numbered_unique
    ON omicron.public.switch_port_settings_bgp_peer_config (port_settings_id, interface_name, addr)
    WHERE addr IS NOT NULL;
