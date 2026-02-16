-- Add an id column to switch_port_settings_bgp_peer_config to serve as the new
-- primary key. This is needed because we're making addr nullable for BGP unnumbered
-- peers, and addr was previously part of the primary key.
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
    ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid();
