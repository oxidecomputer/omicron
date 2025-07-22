CREATE TABLE IF NOT EXISTS omicron.public.inv_ntp_timesync (
    inv_collection_id UUID NOT NULL,
    zone_id UUID NOT NULL,
    synced BOOL NOT NULL,

    PRIMARY KEY (inv_collection_id, zone_id)
);

