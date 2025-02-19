-- Oximeter read policy defaults to reading from a single node ClickHouse server.
INSERT INTO omicron.public.oximeter_read_policy (
    version,
    oximeter_read_mode,
    time_created
) VALUES (
    0,
    'single_node',
    NOW()
) ON CONFLICT DO NOTHING;