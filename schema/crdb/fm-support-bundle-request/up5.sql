CREATE TABLE IF NOT EXISTS omicron.public.fm_sb_req_sp_dumps (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,

    PRIMARY KEY (sitrep_id, request_id)
);
