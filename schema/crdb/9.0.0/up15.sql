CREATE TABLE IF NOT EXISTS omicron.public.inv_caboose (
    inv_collection_id UUID NOT NULL,
    hw_baseboard_id UUID NOT NULL,
    time_collected TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,

    which omicron.public.caboose_which NOT NULL,
    sw_caboose_id UUID NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id, which)
);
