INSERT INTO omicron.public.fm_rendezvous_progress (
    singleton,
    latest_processed_sitrep_version
) VALUES
    (TRUE, 0)
ON CONFLICT DO NOTHING;
