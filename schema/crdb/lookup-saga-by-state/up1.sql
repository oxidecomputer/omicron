CREATE TYPE IF NOT EXISTS omicron.public.stale_saga_state AS ENUM (
    'running',
    'unwinding'
);
