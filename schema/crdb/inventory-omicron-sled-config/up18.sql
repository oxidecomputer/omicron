CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_disk_result (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    disk_id UUID NOT NULL,
    error_message TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, disk_id)
);
