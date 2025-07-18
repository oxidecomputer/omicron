CREATE TABLE IF NOT EXISTS omicron.public.inv_internal_dns (
    inv_collection_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    generation INT8 NOT NULL,
    PRIMARY KEY (inv_collection_id, zone_id)
);

