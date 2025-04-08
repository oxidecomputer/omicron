-- description of a collection of omicron datasets stored in a blueprint
CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_omicron_datasets (
    -- foreign key into the `blueprint` table
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    generation INT8 NOT NULL,

    PRIMARY KEY (blueprint_id, sled_id)
)
