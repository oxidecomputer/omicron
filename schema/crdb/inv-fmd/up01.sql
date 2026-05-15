CREATE TYPE IF NOT EXISTS omicron.public.fmd_inventory_error_kind AS ENUM (
    -- Catch-all for FMD-side failures: daemon unreachable, listing cases
    -- or resources failed, or the platform doesn't have FMD at all. The
    -- accompanying `error_message` carries specifics.
    'fmd_error',
    -- Number of FMD cases reported by the sled exceeded the producer's
    -- limit; no partial data is recorded.
    'too_many_cases',
    -- Number of FMD resources reported by the sled exceeded the limit.
    'too_many_resources'
);

CREATE TABLE IF NOT EXISTS omicron.public.inv_fmd_status (
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,
    -- guaranteed to match a row in this collection's `inv_sled_agent`
    sled_id UUID NOT NULL,
    -- Classifies the failure mode when FMD inventory collection failed.
    -- NULL iff `error_message` is NULL (FMD was successfully collected).
    error_kind omicron.public.fmd_inventory_error_kind,
    -- Display() of the original error; informational only, do not parse.
    -- The `error_kind` discriminator is the structured signal.
    -- NULL iff `error_kind` is NULL.
    error_message TEXT,

    CONSTRAINT error_kind_and_message_together CHECK (
        (error_kind IS NULL) = (error_message IS NULL)
    ),

    PRIMARY KEY (inv_collection_id, sled_id)
);
