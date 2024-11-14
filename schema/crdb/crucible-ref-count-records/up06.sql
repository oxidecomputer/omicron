CREATE INDEX IF NOT EXISTS lookup_volume_resource_usage_by_snapshot on omicron.public.volume_resource_usage (
    region_snapshot_dataset_id, region_snapshot_region_id, region_snapshot_snapshot_id
);
