CREATE TABLE IF NOT EXISTS omicron.public.inv_dataset (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,

    id UUID,

    name TEXT NOT NULL,
    available INT8 NOT NULL,
    used INT8 NOT NULL,
    quota INT8,
    reservation INT8,
    compression TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, name)
);
