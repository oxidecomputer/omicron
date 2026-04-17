ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
  ADD CONSTRAINT IF NOT EXISTS check_router_lifetime_router_lifetime
  CHECK (router_lifetime >= 0 AND router_lifetime <= 9000);
