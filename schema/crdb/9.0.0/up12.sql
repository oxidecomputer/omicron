CREATE TABLE IF NOT EXISTS omicron.public.inv_service_processor (
    inv_collection_id UUID NOT NULL,
    hw_baseboard_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,

    sp_type omicron.public.sp_type NOT NULL,
    sp_slot INT4 NOT NULL,

    baseboard_revision INT8 NOT NULL,
    hubris_archive_id TEXT NOT NULL,
    power_state omicron.public.hw_power_state NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id)
);
