CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_config_reconciler (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    last_reconciled_config UUID NOT NULL,
    boot_disk_slot INT2 CHECK (boot_disk_slot >= 0 AND boot_disk_slot <= 1),
    boot_disk_error TEXT,
    CONSTRAINT boot_disk_slot_or_error CHECK (
        (boot_disk_slot IS NULL AND boot_disk_error IS NOT NULL)
        OR
        (boot_disk_slot IS NOT NULL AND boot_disk_error IS NULL)
    ),
    boot_partition_a_error TEXT,
    boot_partition_b_error TEXT,
    PRIMARY KEY (inv_collection_id, sled_id)
);
