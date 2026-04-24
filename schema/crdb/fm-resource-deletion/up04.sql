CREATE UNIQUE INDEX IF NOT EXISTS one_bundle_per_dataset
    ON omicron.public.support_bundle (dataset_id)
    WHERE time_deleted IS NULL;
