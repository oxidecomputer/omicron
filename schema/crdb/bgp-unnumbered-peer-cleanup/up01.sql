-- Data migration for switch_port_settings_bgp_peer_config
--
-- Normalize "unnumbered peer" addr representations: the sentinel values
-- 0.0.0.0 and :: must be replaced with NULL. Where a
-- (port_settings_id, interface_name) group has multiple
-- sentinel rows, keep only one (preferring NULL, then 0.0.0.0).
-- Also enforce router_lifetime invariants that will be added as CHECK
-- constraints in later migration steps.

SET LOCAL disallow_full_table_scans = off;

-- 1. In groups that already have a NULL addr row, delete redundant sentinel
-- rows (0.0.0.0 or ::).
DELETE FROM omicron.public.switch_port_settings_bgp_peer_config
WHERE (host(addr) = '0.0.0.0' OR host(addr) = '::')
  AND EXISTS (
    SELECT 1
      FROM omicron.public.switch_port_settings_bgp_peer_config AS other
     WHERE other.port_settings_id IS NOT DISTINCT FROM
           switch_port_settings_bgp_peer_config.port_settings_id
       AND other.interface_name IS NOT DISTINCT FROM
           switch_port_settings_bgp_peer_config.interface_name
       AND other.addr IS NULL
  );

-- 2. In groups where both 0.0.0.0 and :: exist (with no NULL row, since those
-- groups were handled above), delete the :: row so that the 0.0.0.0 row can
-- be updated to NULL in step 3.
DELETE FROM omicron.public.switch_port_settings_bgp_peer_config
WHERE host(addr) = '::'
  AND EXISTS (
    SELECT 1
      FROM omicron.public.switch_port_settings_bgp_peer_config AS other
     WHERE other.port_settings_id IS NOT DISTINCT FROM
           switch_port_settings_bgp_peer_config.port_settings_id
       AND other.interface_name IS NOT DISTINCT FROM
           switch_port_settings_bgp_peer_config.interface_name
       AND host(other.addr) = '0.0.0.0'
  );

-- 3. Update all remaining sentinel rows to NULL. After the deletes above,
-- each group has at most one sentinel row.
UPDATE omicron.public.switch_port_settings_bgp_peer_config
   SET addr = NULL
 WHERE host(addr) IN ('0.0.0.0', '::');

-- 4. For numbered peers (non-NULL addr), force router_lifetime to 0.
UPDATE omicron.public.switch_port_settings_bgp_peer_config
   SET router_lifetime = 0
 WHERE addr IS NOT NULL
   AND router_lifetime != 0;

-- 5. For unnumbered peers (NULL addr), clamp router_lifetime to [0, 9000].
UPDATE omicron.public.switch_port_settings_bgp_peer_config
   SET router_lifetime = LEAST(GREATEST(router_lifetime, 0), 9000)
 WHERE addr IS NULL
   AND (router_lifetime < 0 OR router_lifetime > 9000);
