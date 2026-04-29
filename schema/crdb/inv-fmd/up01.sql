CREATE TABLE IF NOT EXISTS omicron.public.inv_fmd_status (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    -- NULL when FMD data was successfully collected. Set to the error
    -- string when FMD collection failed (e.g. on non-illumos sleds, or
    -- when the daemon was unreachable).
    error_message TEXT,

    PRIMARY KEY (inv_collection_id, sled_id)
);
