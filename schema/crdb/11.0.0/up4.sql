CREATE TABLE IF NOT EXISTS omicron.public.inv_root_of_trust_page (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- which system this SP reports it is part of
    -- (foreign key into `hw_baseboard_id` table)
    hw_baseboard_id UUID NOT NULL,
    -- when this observation was made
    time_collected TIMESTAMPTZ NOT NULL,
    -- which MGS instance reported this data
    source TEXT NOT NULL,

    which omicron.public.root_of_trust_page_which NOT NULL,
    sw_root_of_trust_page_id UUID NOT NULL,

    PRIMARY KEY (inv_collection_id, hw_baseboard_id, which)
);
