ALTER TABLE omicron.public.region_snapshot_replacement ADD CONSTRAINT IF NOT EXISTS proper_replacement_fields CHECK (
 (
  (replacement_type = 'region_snapshot') AND
  ((old_dataset_id IS NOT NULL) AND (old_snapshot_id IS NOT NULL))
 ) OR (
  (replacement_type = 'read_only_region') AND
  ((old_dataset_id IS NULL) AND (old_snapshot_id IS NULL))
 )
);
