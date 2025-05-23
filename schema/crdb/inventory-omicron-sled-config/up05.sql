CREATE TYPE IF NOT EXISTS omicron.public.inv_config_reconciler_status_kind
AS ENUM (
    'not-yet-run',
    'running',
    'idle'
);
