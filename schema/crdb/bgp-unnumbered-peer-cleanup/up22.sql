ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config_allow_export
    ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid();
