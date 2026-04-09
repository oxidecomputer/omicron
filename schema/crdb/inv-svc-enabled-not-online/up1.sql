CREATE TYPE IF NOT EXISTS omicron.public.inv_svc_state AS ENUM (
    'uninitialized',
    'offline',
    'online',
    'degraded',
    'maintenance',
    'disabled',
    'legacy_run',
    'unknown'
);
