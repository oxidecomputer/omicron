set local disallow_full_table_scans = off;

-- Fill in the external IP IDs for all past blueprints.
--
-- This query makes some assumptions that are true at the time of its writing
-- for systems where this migration will run, but may not be true in the future
-- or for other systems:
--
-- 1. We've never deleted an Omicron zone external IP. (This will be untrue
--    _soon_, as the driver for this migration is to do exactly that.)
-- 2. Only the three zone types listed below have external IPs.
-- 3. Every blueprint zone of one of those three types has exactly one external
--    IP.
-- 4. We do not have any blueprints that have not yet been realized. (If we did,
--    those zones would not have corresponding external IPs. We'd leave the IPs
--    as NULL, which would prevent them from being loaded from the db.)
UPDATE bp_omicron_zone SET external_ip_id = (
   SELECT external_ip.id FROM external_ip
       WHERE external_ip.parent_id = bp_omicron_zone.id
       AND   time_deleted IS NULL
)
WHERE zone_type IN ('nexus','external_dns','boundary_ntp');
