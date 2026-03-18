CREATE TABLE IF NOT EXISTS omicron.public.fm_sb_req_host_info (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    all_sleds BOOL NOT NULL,
    sled_ids UUID[] NOT NULL DEFAULT ARRAY[],

    PRIMARY KEY (sitrep_id, request_id)
);
