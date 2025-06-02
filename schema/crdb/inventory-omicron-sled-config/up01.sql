-- Delete all existing inventory collections. Inventory collections are
-- routinely pruned by Nexus, so this should only delete a handful (3-6
-- collections total). It does mean we will have no collections until Nexus
-- collects a new one after this migration completes, but that should happen
-- soon after Nexus starts and is not a prerequisite for the control plane
-- coming online.

set local disallow_full_table_scans = off;

-- This omits a few tables because we're about to drop them entirely anyway.
DELETE FROM omicron.public.inv_collection WHERE 1=1;
DELETE FROM omicron.public.inv_collection_error WHERE 1=1;
DELETE FROM omicron.public.inv_service_processor WHERE 1=1;
DELETE FROM omicron.public.inv_root_of_trust WHERE 1=1;
DELETE FROM omicron.public.inv_caboose WHERE 1=1;
DELETE FROM omicron.public.inv_root_of_trust_page WHERE 1=1;
DELETE FROM omicron.public.inv_sled_agent WHERE 1=1;
DELETE FROM omicron.public.inv_physical_disk WHERE 1=1;
DELETE FROM omicron.public.inv_nvme_disk_firmware WHERE 1=1;
DELETE FROM omicron.public.inv_zpool WHERE 1=1;
DELETE FROM omicron.public.inv_dataset WHERE 1=1;
DELETE FROM omicron.public.inv_clickhouse_keeper_membership WHERE 1=1;
