ALTER TABLE omicron.public.router_configuration_bgp_peer
  ADD CONSTRAINT IF NOT EXISTS src_addr_only_for_numbered_peers
  CHECK (src_addr IS NULL OR addr IS NOT NULL);
