CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_deleting on omicron.public.region_snapshot (
    deleting
);
