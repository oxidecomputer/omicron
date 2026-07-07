-- A table describing a single measurement file within a measurement manifest
-- collected by inventory
CREATE TABLE IF NOT EXISTS omicron.public.inv_zone_manifest_measurement (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    sled_id UUID NOT NULL,

    -- measurement file name, part of the primary key within this table.
    measurement_file_name TEXT NOT NULL,

    -- measurement file name, part of the primary key within this table.
    path TEXT NOT NULL,

    -- The expected file size.
    expected_size INT8 NOT NULL,

    -- The expected hash.
    expected_sha256 STRING(64) NOT NULL,

    -- The error while reading the zone or matching it to the manifest, if any.
    -- NULL indicates success.
    error TEXT,
    PRIMARY KEY (inv_collection_id, sled_id, measurement_file_name)
);

