CREATE INDEX IF NOT EXISTS lookup_local_storage_unencrypted_dataset_by_zpool ON
    omicron.public.rendezvous_local_storage_unencrypted_dataset (pool_id, id)
  WHERE time_tombstoned IS NULL;
