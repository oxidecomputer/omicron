CREATE TABLE IF NOT EXISTS omicron.public.inv_physical_disk (
    inv_collection_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    slot INT8 CHECK (slot >= 0) NOT NULL,

    vendor STRING(63) NOT NULL,
    model STRING(63) NOT NULL,
    serial STRING(63) NOT NULL,

    variant omicron.public.physical_disk_kind NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, slot)
);
