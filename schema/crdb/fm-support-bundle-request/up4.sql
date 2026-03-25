CREATE TABLE IF NOT EXISTS omicron.public.fm_support_bundle_data_reconfigurator (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,

    PRIMARY KEY (sitrep_id, request_id)
);
