CREATE UNIQUE INDEX IF NOT EXISTS one_record_per_volume_resource_usage on omicron.public.volume_resource_usage (
    volume_id,
    usage_type,
    region_id,
    region_snapshot_dataset_id,
    region_snapshot_region_id,
    region_snapshot_snapshot_id
);
