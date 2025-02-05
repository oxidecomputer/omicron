CREATE INDEX IF NOT EXISTS lookup_crucible_dataset_by_zpool ON
    omicron.public.crucible_dataset (pool_id, id)
  WHERE time_deleted IS NULL;
