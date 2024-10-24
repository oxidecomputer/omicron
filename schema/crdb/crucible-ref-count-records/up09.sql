CREATE INDEX IF NOT EXISTS lookup_regions_by_read_only
    on omicron.public.region (read_only);
