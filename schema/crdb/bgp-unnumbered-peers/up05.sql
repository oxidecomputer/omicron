-- Add unique constraint for unnumbered peers (those without addr).
-- Only one unnumbered peer is allowed per interface.
CREATE UNIQUE INDEX IF NOT EXISTS switch_port_settings_bgp_peer_config_unnumbered_unique
    ON omicron.public.switch_port_settings_bgp_peer_config (port_settings_id, interface_name)
    WHERE addr IS NULL;
