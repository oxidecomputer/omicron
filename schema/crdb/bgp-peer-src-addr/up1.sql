-- Add an optional source address column to the BGP peer config table.
-- This allows operators to specify the local IP address that the router
-- should use when initiating BGP sessions with a peer.
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
    ADD COLUMN IF NOT EXISTS src_addr INET;
