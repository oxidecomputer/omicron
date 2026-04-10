CREATE TYPE IF NOT EXISTS omicron.public.inv_svc_enabled_not_online_state AS ENUM (
    'uninitialized',
    'offline',
    'degraded',
    'maintenance',
    'unknown'
);
