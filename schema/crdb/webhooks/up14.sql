-- Singleton probe event
INSERT INTO omicron.public.webhook_event (
    id,
    time_created,
    time_modified,
    event_class,
    event,
    time_dispatched,
    num_dispatched
) VALUES (
    -- NOTE: this UUID is duplicated in nexus_db_model::webhook_event.
    '001de000-7768-4000-8000-000000000001',
    NOW(),
    NOW(),
    'probe',
    '{}',
    -- Pretend to be dispatched so we won't show up in "list events needing
    -- dispatch" queries
    NOW(),
    0
) ON CONFLICT DO NOTHING;
