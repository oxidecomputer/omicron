ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
  ADD CONSTRAINT IF NOT EXISTS src_addr_only_for_numbered_peers
  CHECK (src_addr IS NULL OR addr IS NOT NULL);
