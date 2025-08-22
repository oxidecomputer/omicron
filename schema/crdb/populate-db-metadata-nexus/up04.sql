-- Populate db_metadata_nexus records for all Nexus zones in the current target blueprint
--
-- This migration handles backfill for existing deployments that are upgrading
-- to include db_metadata_nexus. It finds all Nexus zones in the current
-- target blueprint and marks them as 'active' in the db_metadata_nexus table.

SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.db_metadata_nexus (nexus_id, last_drained_blueprint_id, state)
SELECT bz.id, NULL, 'active'
FROM omicron.public.bp_target bt
JOIN omicron.public.bp_omicron_zone bz ON bt.blueprint_id = bz.blueprint_id
WHERE bz.zone_type = 'nexus'
  AND bt.version = (SELECT MAX(version) FROM omicron.public.bp_target)
ON CONFLICT (nexus_id) DO UPDATE SET state = EXCLUDED.state;
