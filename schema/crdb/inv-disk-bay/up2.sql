CREATE TABLE IF NOT EXISTS omicron.public.inv_disk_bay (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    location STRING(63) NOT NULL,
    kind omicron.public.physical_disk_kind NOT NULL,
    occupant omicron.public.inv_disk_bay_occupant NOT NULL,
    disk_vendor STRING(63),
    disk_model STRING(63),
    disk_serial STRING(63),
    device_driver STRING(63),
    device_devfs_path TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, location)
);
