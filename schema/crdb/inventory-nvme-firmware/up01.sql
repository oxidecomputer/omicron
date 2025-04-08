CREATE TABLE IF NOT EXISTS omicron.public.inv_nvme_disk_firmware (
    inv_collection_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    slot INT8 CHECK (slot >= 0) NOT NULL,

    number_of_slots INT2 CHECK (number_of_slots BETWEEN 1 AND 7) NOT NULL,
    active_slot INT2 CHECK (active_slot BETWEEN 1 AND 7) NOT NULL,
    next_active_slot INT2 CHECK (next_active_slot BETWEEN 1 AND 7),
    slot1_is_read_only BOOLEAN,
    slot_firmware_versions STRING(8)[] CHECK (array_length(slot_firmware_versions, 1) BETWEEN 1 AND 7),

    PRIMARY KEY (inv_collection_id, sled_id, slot)
);
