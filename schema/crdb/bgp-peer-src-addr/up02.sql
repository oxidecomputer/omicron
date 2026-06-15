-- Drop bgp_peer_view so it can be recreated with the src_addr column in up03.sql.
DROP VIEW IF EXISTS omicron.public.bgp_peer_view;
