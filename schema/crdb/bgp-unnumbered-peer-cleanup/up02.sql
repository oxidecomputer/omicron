ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
  ADD CONSTRAINT IF NOT EXISTS check_addr_addr
  CHECK (host(addr) != '0.0.0.0' AND host(addr) != '::');
