INSERT INTO omicron.public.fm_config (
    version,
    sitrep_limit,
    sitrep_deletion_threshold,
    time_modified
) VALUES
    (1, 1000, 900, NOW())
ON CONFLICT DO NOTHING;
