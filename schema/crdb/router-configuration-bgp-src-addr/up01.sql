ALTER TABLE omicron.public.router_configuration_bgp_peer
    ADD COLUMN IF NOT EXISTS src_addr INET CHECK (host(src_addr) != '0.0.0.0' AND host(src_addr) != '::');
