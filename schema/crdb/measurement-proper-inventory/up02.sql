-- Move from the old table to the new

set local disallow_full_table_scans = off;

INSERT INTO omicron.public.inv_single_measurements (
    inv_collection_id,
    sled_id,
    path
)
SELECT
   inv_collection_id,
   sled_id,
   path
FROM
   omicron.public.inv_last_reconciliation_measurements;
