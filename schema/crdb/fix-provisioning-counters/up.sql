-- This change fixes provisioning counters, alongside the
-- underflow fix provided in https://github.com/oxidecomputer/omicron/pull/5830.
-- Although this underflow has been fixed, it could have resulted
-- in invalid accounting, which is mitigated by this schema change.
--
-- This update is currently occurring offline, so we exploit
-- that fact to identify that all instances should be off.

SET LOCAL disallow_full_table_scans = OFF;

-- First, ensure that no instance records exist.
DELETE FROM omicron.public.virtual_provisioning_resource
WHERE resource_type='instance';

-- Next, update the collections to identify that there
-- are no instances running.
UPDATE omicron.public.virtual_provisioning_collection
SET
    cpus_provisioned = 0,
    ram_provisioned = 0;

