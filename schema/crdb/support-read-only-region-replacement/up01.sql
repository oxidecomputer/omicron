CREATE INDEX IF NOT EXISTS lookup_snapshot_by_volume_id
    ON omicron.public.snapshot ( volume_id );
