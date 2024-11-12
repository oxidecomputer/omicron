CREATE INDEX IF NOT EXISTS lookup_volume_resource_usage_by_region on omicron.public.volume_resource_usage (
    region_id
);
