ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config ADD COLUMN IF NOT EXISTS allow_import_list_active BOOLEAN NOT NULL DEFAULT false;
