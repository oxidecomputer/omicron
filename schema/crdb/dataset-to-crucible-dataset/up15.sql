CREATE INDEX IF NOT EXISTS lookup_crucible_dataset_by_size_used ON
    omicron.public.crucible_dataset (size_used)
  WHERE time_deleted IS NULL;
