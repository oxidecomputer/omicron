-- This update is currently occurring offline, so we exploit
-- that fact to identify that all instances should be offline.

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

