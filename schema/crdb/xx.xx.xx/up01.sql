CREATE TYPE IF NOT EXISTS omicron.public.physical_disk_state AS ENUM (
    -- The disk is actively being used, and should be a target
    -- for future allocations.
    'active',
    -- The disk may still be in usage, but should not be used
    -- for subsequent allocations.
    --
    -- This state could be set when we have, for example, datasets
    -- actively being used by the disk which we haven't fully retired.
    'draining',
    -- The disk is not currently being used.
    'inactive'
);
