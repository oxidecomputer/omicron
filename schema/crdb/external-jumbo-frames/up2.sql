INSERT INTO omicron.public.system_networking_settings (
    singleton,
    external_jumbo_frames_opt_in_enabled
) VALUES (
    TRUE, FALSE
) ON CONFLICT DO NOTHING;
