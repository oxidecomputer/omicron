-- Stop tracking zpools on M.2s
-- We only want to know about M.2s in the inventory, for now.
SET LOCAL disallow_full_table_scans = off;
DELETE FROM omicron.public.zpool
WHERE physical_disk_id IN (
    SELECT id
    FROM omicron.public.physical_disk
    WHERE variant = 'm2'
);
