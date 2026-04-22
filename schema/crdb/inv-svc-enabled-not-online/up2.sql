CREATE TABLE IF NOT EXISTS omicron.public.inv_svc_enabled_not_online (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    id UUID NOT NULL,
    svcs_cmd_error TEXT,
    time_of_status TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (inv_collection_id, sled_id, id)
);
