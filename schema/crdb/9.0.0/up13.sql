CREATE TABLE IF NOT EXISTS omicron.public.inv_root_of_trust (
    inv_collection_id UUID NOT NULL,
    hw_baseboard_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,

    slot_active omicron.public.hw_rot_slot NOT NULL,
    slot_boot_pref_transient omicron.public.hw_rot_slot,
    slot_boot_pref_persistent omicron.public.hw_rot_slot NOT NULL,
    slot_boot_pref_persistent_pending omicron.public.hw_rot_slot,
    slot_a_sha3_256 TEXT,
    slot_b_sha3_256 TEXT,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);
