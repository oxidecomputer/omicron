ALTER TABLE omicron.public.region_snapshot_replacement
    ADD COLUMN IF NOT EXISTS replacement_type omicron.public.read_only_target_replacement_type NOT NULL DEFAULT 'region_snapshot';
