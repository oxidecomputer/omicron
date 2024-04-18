-- description of omicron physical disks specified in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_physical_disk  (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,

    -- unique id for this sled (should be foreign keys into `sled` table, though
    -- it's conceivable a blueprint could refer to a sled that no longer exists,
    -- particularly if the blueprint is older than the current target)
    sled_id UUID NOT NULL,

    vendor TEXT NOT NULL,
    serial TEXT NOT NULL,
    model TEXT NOT NULL,

    id UUID NOT NULL,
    pool_id UUID NOT NULL,

    PRIMARY KEY (blueprint_id, id)
);

