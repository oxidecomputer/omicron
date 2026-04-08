CREATE TABLE IF NOT EXISTS omicron.public.support_bundle_data_selection_ereports (
    bundle_id UUID NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    only_serials TEXT[] NOT NULL DEFAULT ARRAY[],
    only_classes TEXT[] NOT NULL DEFAULT ARRAY[],

    PRIMARY KEY (bundle_id),
    CHECK (start_time IS NULL OR end_time IS NULL OR start_time <= end_time)
);
