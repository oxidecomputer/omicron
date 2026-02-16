-- add a column for configuring router lifetime for unnumbered BGP peers

ALTER TABLE IF EXISTS omicron.public.switch_port_settings_bgp_peer_config
  ADD COLUMN IF NOT EXISTS router_lifetime INT4 NOT NULL DEFAULT 0;
