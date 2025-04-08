CREATE INDEX IF NOT EXISTS lookup_region_snapshot_by_snapshot_id on omicron.public.region_snapshot (
    snapshot_id
);
