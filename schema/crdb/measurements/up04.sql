CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_measurements (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL,
    error_message TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, file_name)
);

