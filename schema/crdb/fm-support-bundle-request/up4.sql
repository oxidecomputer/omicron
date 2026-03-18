CREATE TABLE IF NOT EXISTS omicron.public.fm_sb_req_sled_cubby_info (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,

    PRIMARY KEY (sitrep_id, request_id)
);
