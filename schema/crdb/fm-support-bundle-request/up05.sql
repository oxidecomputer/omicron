CREATE TABLE IF NOT EXISTS omicron.public.fm_support_bundle_request_data_selection_host_info (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    all_sleds BOOL NOT NULL,
    sled_ids UUID[] NOT NULL DEFAULT ARRAY[],

    PRIMARY KEY (sitrep_id, request_id),
    CONSTRAINT all_sleds_and_specific_sleds_are_mutually_exclusive CHECK (NOT (all_sleds AND cardinality(sled_ids) > 0))
);
