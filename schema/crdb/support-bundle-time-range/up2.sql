CREATE TABLE IF NOT EXISTS omicron.public.fm_support_bundle_request_data_selection_time_range (
    sitrep_id UUID NOT NULL,
    request_id UUID NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    PRIMARY KEY (sitrep_id, request_id),
    CONSTRAINT start_before_end CHECK (
        start_time IS NULL OR end_time IS NULL OR start_time <= end_time
    )
);
