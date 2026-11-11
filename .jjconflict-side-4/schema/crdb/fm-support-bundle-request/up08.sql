CREATE TABLE IF NOT EXISTS omicron.public.support_bundle_data_selection_flags (
    bundle_id UUID NOT NULL,
    include_reconfigurator BOOL NOT NULL,
    include_sled_cubby_info BOOL NOT NULL,
    include_sp_dumps BOOL NOT NULL,

    PRIMARY KEY (bundle_id)
);
