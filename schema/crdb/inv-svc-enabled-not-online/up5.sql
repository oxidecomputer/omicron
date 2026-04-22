CREATE TABLE IF NOT EXISTS omicron.public.inv_svc_enabled_not_online_parse_error (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,
    error_message TEXT NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);
