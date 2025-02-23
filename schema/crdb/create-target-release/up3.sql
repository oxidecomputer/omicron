-- System software is by default from the `install` dataset.
INSERT INTO omicron.public.target_release (
    generation,
    time_requested,
    release_source,
    tuf_repo_id
) VALUES (
    1,
    NOW(),
    'unspecified',
    NULL
) ON CONFLICT DO NOTHING;
