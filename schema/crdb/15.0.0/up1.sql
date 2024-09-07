CREATE TYPE IF NOT EXISTS omicron.public.sled_provision_state AS ENUM (
    -- New resources can be provisioned onto the sled
    'provisionable',
    -- New resources must not be provisioned onto the sled
    'non_provisionable'
);
