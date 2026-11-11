CREATE TYPE IF NOT EXISTS omicron.public.fmd_inventory_error_kind AS ENUM (
    -- Catch-all for FMD-side failures: daemon unreachable, listing cases
    -- or resources failed, or the platform doesn't have FMD at all. The
    -- accompanying `error_message` carries specifics.
    'fmd_error',
    -- Number of FMD cases reported by the sled exceeded the producer's
    -- limit; no partial data is recorded.
    'too_many_cases',
    -- Number of FMD resources reported by the sled exceeded the limit;
    -- no partial data is recorded.
    'too_many_resources'
);
