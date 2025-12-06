CREATE INDEX IF NOT EXISTS
  lookup_local_storage_dataset_allocation_by_dataset
ON
  omicron.public.local_storage_dataset_allocation (local_storage_dataset_id)
WHERE
  time_deleted IS NULL;
