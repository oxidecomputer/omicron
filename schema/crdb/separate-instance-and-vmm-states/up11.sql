CREATE TYPE IF NOT EXISTS omicron.public.vmm_state AS ENUM (
    'starting',
    'running',
    'stopping',
    'stopped',
    'rebooting',
    'migrating',
    'failed',
    'destroyed'
);
