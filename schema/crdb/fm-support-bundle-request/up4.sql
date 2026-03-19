CREATE TABLE IF NOT EXISTS omicron.public.fm_sb_req_data_selection (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    category omicron.public.bundle_data_category NOT NULL,

    -- HostInfo fields (non-null iff category = 'host_info')
    all_sleds BOOL,
    sled_ids UUID[],

    -- Ereports fields (non-null iff category = 'ereports')
    ereport_start_time TIMESTAMPTZ,
    ereport_end_time TIMESTAMPTZ,
    ereport_only_serials TEXT[],
    ereport_only_classes TEXT[],

    PRIMARY KEY (sitrep_id, request_id, category),

    -- HostInfo: both fields present iff category = 'host_info'.
    CHECK ((category = 'host_info') = (all_sleds IS NOT NULL)),
    CHECK ((category = 'host_info') = (sled_ids IS NOT NULL)),
    -- all_sleds = true means no specific sled IDs, and vice versa.
    CHECK (all_sleds IS NULL OR all_sleds = (array_length(sled_ids, 1) IS NULL)),

    -- Ereports: serials and classes present iff category = 'ereports'.
    CHECK ((category = 'ereports') = (ereport_only_serials IS NOT NULL)),
    CHECK ((category = 'ereports') = (ereport_only_classes IS NOT NULL)),
    -- Time bounds are optional within ereports, but must be NULL otherwise.
    CHECK (category = 'ereports' OR ereport_start_time IS NULL),
    CHECK (category = 'ereports' OR ereport_end_time IS NULL)
);
