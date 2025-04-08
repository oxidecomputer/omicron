CREATE TABLE IF NOT EXISTS omicron.public.bp_omicron_zones_not_in_service (
    blueprint_id UUID NOT NULL,
    bp_omicron_zone_id UUID NOT NULL,

    PRIMARY KEY (blueprint_id, bp_omicron_zone_id)
);
