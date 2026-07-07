CREATE TABLE IF NOT EXISTS omicron.public.inv_single_measurements (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,

    -- full path to the measurement file
    path TEXT NOT NULL,
    
    -- error message; if NULL, an "ok" result
    error_message TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, path)
);

