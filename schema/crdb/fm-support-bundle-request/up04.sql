CREATE TABLE IF NOT EXISTS omicron.public.fm_support_bundle_request_data_selection_flags (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    include_reconfigurator BOOL NOT NULL,
    include_sled_cubby_info BOOL NOT NULL,
    include_sp_dumps BOOL NOT NULL,

    PRIMARY KEY (sitrep_id, request_id)
);
