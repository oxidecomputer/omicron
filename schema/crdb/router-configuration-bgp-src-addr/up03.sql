ALTER TABLE omicron.public.router_configuration_bgp_peer
  ADD CONSTRAINT IF NOT EXISTS src_addr_family_must_match_peer
  CHECK (family(src_addr) = family(addr));
