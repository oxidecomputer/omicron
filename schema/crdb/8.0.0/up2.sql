ALTER TABLE omicron.public.bgp_config ADD COLUMN IF NOT EXISTS bgp_announce_set_id UUID NOT NULL;
ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config DROP COLUMN IF EXISTS bgp_announce_set_id;
