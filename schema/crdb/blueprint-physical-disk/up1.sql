-- description of a collection of omicron physical disks stored in a blueprint.
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_omicron_physical_disks (
    -- foreign key into `blueprint` table
    blueprint_id UUID NOT NULL,

    sled_id UUID NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (blueprint_id, sled_id)
);

