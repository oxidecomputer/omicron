CREATE TABLE IF NOT EXISTS omicron.public.inv_svc_enabled_not_online_service (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,
    fmri TEXT NOT NULL,
    zone TEXT NOT NULL,
    state omicron.public.inv_svc_state NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);
