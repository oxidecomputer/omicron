-- System software is by default from the `install` dataset.
INSERT INTO omicron.public.target_release (
    generation,
    time_requested,
    release_source,
    system_version
) VALUES (
    0,
    NOW(),
    'install_dataset',
    NULL
) ON CONFLICT DO NOTHING;
