-- Default: 10% free datasets, keep at least 10% worth of bundles
INSERT INTO omicron.public.support_bundle_config (
    singleton,
    target_free_percent,
    min_keep_percent,
    time_modified
)
VALUES (TRUE, 10, 10, NOW())
ON CONFLICT (singleton) DO NOTHING;
