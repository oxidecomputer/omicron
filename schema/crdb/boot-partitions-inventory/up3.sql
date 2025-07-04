SET LOCAL disallow_full_table_scans = off;

-- Move any non-NULL `last_reconciliation_sled_config` values out of
-- `inv_sled_agent` and into new rows in `inv_sled_config_reconciler`. We fill
-- in the rest of the columns with dummy errors for old collections that don't
-- have data.
INSERT INTO omicron.public.inv_sled_config_reconciler (
    inv_collection_id,
    sled_id,
    last_reconciled_config,
    boot_disk_slot,
    boot_disk_error,
    boot_partition_a_error,
    boot_partition_b_error
)

SELECT
    inv_collection_id,
    sled_id,
    last_reconciliation_sled_config,
    NULL, -- boot_disk_slot
    'old collection, data missing', -- boot_disk_error
    'old collection, data missing', -- boot_partition_a_error
    'old collection, data missing'  -- boot_partition_b_error
FROM omicron.public.inv_sled_agent
WHERE last_reconciliation_sled_config IS NOT NULL

-- This is a new table we just created; the only way to get conflicts is if
-- we're rerunning this migration. Do nothing to make it idempotent.
ON CONFLICT DO NOTHING;
