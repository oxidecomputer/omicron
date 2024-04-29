CREATE TABLE IF NOT EXISTS omicron.public.bp_sled_state (
    blueprint_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    sled_state omicron.public.sled_state NOT NULL,
    PRIMARY KEY (blueprint_id, sled_id)
);
