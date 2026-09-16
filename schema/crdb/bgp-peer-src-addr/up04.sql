ALTER TABLE omicron.public.switch_port_settings_bgp_peer_config
  ADD CONSTRAINT IF NOT EXISTS src_addr_family_must_match_peer
  CHECK (family(src_addr) = family(addr));
