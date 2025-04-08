CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_omicron_zones (
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (blueprint_id, sled_id)
);
