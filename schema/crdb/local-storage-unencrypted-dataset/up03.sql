CREATE INDEX IF NOT EXISTS lookup_local_storage_unencrypted_dataset_by_size_used ON
    omicron.public.rendezvous_local_storage_unencrypted_dataset (size_used)
  WHERE time_tombstoned IS NULL;

