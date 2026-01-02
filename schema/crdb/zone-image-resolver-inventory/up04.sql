-- Create table for zone manifest zone inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_zone_manifest_zone (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    zone_file_name TEXT NOT NULL,
    path TEXT NOT NULL,
    expected_size INT8 NOT NULL,
    expected_sha256 STRING(64) NOT NULL,
    error TEXT,

    PRIMARY KEY (inv_collection_id, sled_id, zone_file_name)
);
