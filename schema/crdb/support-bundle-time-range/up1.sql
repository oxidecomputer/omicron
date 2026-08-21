CREATE TABLE IF NOT EXISTS omicron.public.support_bundle_data_selection_time_range (
    bundle_id UUID NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    PRIMARY KEY (bundle_id),
    CONSTRAINT start_before_end CHECK (
        start_time IS NULL OR end_time IS NULL OR start_time <= end_time
    )
);
