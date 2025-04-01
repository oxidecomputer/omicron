DELETE FROM sled_resource_vmm
WHERE NOT EXISTS (
    SELECT 1
    FROM vmm
    WHERE vmm.id = sled_resource_vmm.id
);
