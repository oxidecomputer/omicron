CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_dataset_result (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    dataset_id UUID NOT NULL,
    error_message TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, dataset_id)
);
