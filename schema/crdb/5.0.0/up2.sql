CREATE INDEX IF NOT EXISTS lookup_dataset_by_zpool ON omicron.public.dataset (pool_id, id) WHERE pool_id IS NOT NULL AND time_deleted IS NULL;
