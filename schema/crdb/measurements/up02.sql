-- Create table for measurement manifest non-boot disk inventory.
CREATE TABLE IF NOT EXISTS omicron.public.inv_measurement_manifest_non_boot (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a sled will report an id that we don't know about)
    sled_id UUID NOT NULL,
    -- unique ID for this non-boot disk
    non_boot_zpool_id UUID NOT NULL,
    -- The full path to the zone manifest.
    path TEXT NOT NULL,
    -- Whether the non-boot disk is in a valid state.
    is_valid BOOLEAN NOT NULL,
    -- A message attached to this disk.
    message TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, non_boot_zpool_id)
);
