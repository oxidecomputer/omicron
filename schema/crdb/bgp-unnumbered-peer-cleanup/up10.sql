-- Data migration for switch_port_settings_bgp_peer_config_communities
--
-- Convert sentinel addr values (0.0.0.0, ::) to NULL for unnumbered peers.
-- Where both sentinels exist for the same (port_settings_id, interface_name,
-- community) tuple, prefer the 0.0.0.0 row (update it to NULL) and delete
-- the :: row.

SET LOCAL disallow_full_table_scans = off;

-- 1. Delete :: rows that have a matching 0.0.0.0 row for the same community.
DELETE FROM omicron.public.switch_port_settings_bgp_peer_config_communities
WHERE host(addr) = '::'
  AND EXISTS (
    SELECT 1
      FROM omicron.public.switch_port_settings_bgp_peer_config_communities
               AS other
     WHERE other.port_settings_id =
           switch_port_settings_bgp_peer_config_communities.port_settings_id
       AND other.interface_name =
           switch_port_settings_bgp_peer_config_communities.interface_name
       AND other.community =
           switch_port_settings_bgp_peer_config_communities.community
       AND host(other.addr) = '0.0.0.0'
  );

-- 2. Update all remaining sentinel rows to NULL.
UPDATE omicron.public.switch_port_settings_bgp_peer_config_communities
   SET addr = NULL
 WHERE host(addr) IN ('0.0.0.0', '::');
