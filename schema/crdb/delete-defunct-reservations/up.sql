SET LOCAL disallow_full_table_scans = off;
DELETE FROM sled_resource_vmm
WHERE NOT EXISTS (
    SELECT 1
    FROM vmm
    WHERE vmm.id = sled_resource_vmm.id
);
SET LOCAL disallow_full_table_scans = on;
