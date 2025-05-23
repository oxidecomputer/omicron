CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_zone_result (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    error_message TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, zone_id)
);
