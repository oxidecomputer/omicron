-- Create table for mupdate override non-boot disk inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_mupdate_override_non_boot (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    non_boot_zpool_id UUID NOT NULL,
    path TEXT NOT NULL,
    is_valid BOOLEAN NOT NULL,
    message TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, non_boot_zpool_id)
);
