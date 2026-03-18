CREATE TABLE IF NOT EXISTS omicron.public.fm_sb_req_ereports (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    only_serials TEXT[] NOT NULL DEFAULT ARRAY[],
    only_classes TEXT[] NOT NULL DEFAULT ARRAY[],

    PRIMARY KEY (sitrep_id, request_id)
);
