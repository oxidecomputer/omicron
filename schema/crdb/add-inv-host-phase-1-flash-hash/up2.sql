CREATE TABLE IF NOT EXISTS omicron.public.inv_host_phase_1_flash_hash (
    inv_collection_id UUID NOT NULL,
    hw_baseboard_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    slot omicron.public.hw_m2_slot NOT NULL,
    hash STRING(64) NOT NULL,
    PRIMARY KEY (inv_collection_id, hw_baseboard_id, slot)
);
