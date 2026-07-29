ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
    ADD COLUMN IF NOT EXISTS src_addr INET CHECK (host(src_addr) != '0.0.0.0' AND host(src_addr) != '::');
