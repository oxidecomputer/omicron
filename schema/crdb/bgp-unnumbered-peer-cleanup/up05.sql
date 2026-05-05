ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
  ADD CONSTRAINT IF NOT EXISTS router_lifetime_only_for_unnumbered_peers
  CHECK (router_lifetime = 0 OR addr IS NULL);
