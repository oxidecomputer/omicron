CREATE TABLE IF NOT EXISTS omicron.public.inv_sled_agent (
    inv_collection_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,

    sled_id UUID NOT NULL,

    hw_baseboard_id UUID,

    sled_agent_ip INET NOT NULL,
    sled_agent_port INT4 NOT NULL,
    sled_role omicron.public.sled_role NOT NULL,
    usable_hardware_threads INT8
        CHECK (usable_hardware_threads BETWEEN 0 AND 4294967295) NOT NULL,
    usable_physical_ram INT8 NOT NULL,
    reservoir_size INT8 CHECK (reservoir_size < usable_physical_ram) NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id)
);
