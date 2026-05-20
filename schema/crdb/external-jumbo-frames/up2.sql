INSERT INTO omicron.public.system_networking_settings (
    singleton,
    time_created,
    time_modified,
    external_jumbo_frames_opt_in_enabled
) VALUES (
    TRUE, NOW(), NOW(), FALSE
) ON CONFLICT DO NOTHING;
