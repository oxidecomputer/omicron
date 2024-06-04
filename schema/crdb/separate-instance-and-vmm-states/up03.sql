CREATE TYPE IF NOT EXISTS omicron.public.instance_state_v2 AS ENUM (
    'creating',
    'no_vmm',
    'vmm',
    'failed',
    'destroyed'
);
