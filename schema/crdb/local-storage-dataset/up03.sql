/* Create an index on the size usage for any Crucible dataset */
CREATE INDEX IF NOT EXISTS lookup_local_storage_dataset_by_size_used ON
    omicron.public.local_storage_dataset (size_used)
  WHERE time_deleted IS NULL;
