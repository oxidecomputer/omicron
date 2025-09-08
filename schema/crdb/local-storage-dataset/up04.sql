/* Create an index on the zpool id */
CREATE INDEX IF NOT EXISTS lookup_local_storage_dataset_by_zpool ON
    omicron.public.local_storage_dataset (pool_id, id)
  WHERE time_deleted IS NULL;
