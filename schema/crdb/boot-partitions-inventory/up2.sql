CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_boot_partition (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    boot_disk_slot INT2
        CHECK (boot_disk_slot >= 0 AND boot_disk_slot <= 1) NOT NULL,
    artifact_hash STRING(64) NOT NULL,
    artifact_size INT8 NOT NULL,
    header_flags INT8 NOT NULL,
    header_data_size INT8 NOT NULL,
    header_image_size INT8 NOT NULL,
    header_target_size INT8 NOT NULL,
    header_sha256 STRING(64) NOT NULL,
    header_image_name TEXT NOT NULL,
    PRIMARY KEY (inv_collection_id, sled_id, boot_disk_slot)
);
