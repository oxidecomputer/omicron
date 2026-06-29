CREATE TYPE IF NOT EXISTS omicron.public.vmm_failure_reason AS ENUM (
    'prehistoric',
    'from_sled_agent',
    'no_such_instance',
    'sled_expunged',
    'sled_off'
);
