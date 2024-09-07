CREATE TABLE IF NOT EXISTS omicron.public.inv_collection_error (
    inv_collection_id UUID NOT NULL,
    idx INT4 NOT NULL,
    message TEXT
);
