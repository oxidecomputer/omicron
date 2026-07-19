CREATE TYPE IF NOT EXISTS omicron.public.sled_resource_vmm_state AS ENUM (
    -- A VMM's resources are still used, but it should be garbage collected
    'tombstoned',

    -- This VMM is running the instance
    'active',

    -- This VMM is a migration destination for the active VMM
    'target'
);
