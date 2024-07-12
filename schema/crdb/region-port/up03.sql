CREATE INDEX IF NOT EXISTS lookup_regions_missing_ports
    on omicron.public.region (id)
    WHERE port IS NULL;
