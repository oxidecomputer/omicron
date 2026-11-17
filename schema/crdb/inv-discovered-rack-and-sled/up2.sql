-- Inventory of sleds on other racks discovered via the multirack join service.
CREATE TABLE IF NOT EXISTS omicron.public.inv_discovered_sled (
    -- where this observation came from
    -- (foreign key into `inv_collection` table)
    inv_collection_id UUID NOT NULL,

    -- This is the same id the sled-agent uses to upsert itself into the `sled`
    -- table if/when the sled is adopted:
    sled_id UUID NOT NULL,

    -- Foreign key into `inv_discovered_rack` for the same collection:
    rack_id UUID NOT NULL,

    -- Baseboard info about the sled:
    part_number STRING(63) NOT NULL,
    serial_number STRING(63) NOT NULL,

    -- The IPv6 underlay /64 prefix for the sled:
    sled_subnet INET NOT NULL,

    -- Whether the join service reports the sled-agent as started:
    sled_agent_started BOOL NOT NULL,
    -- The most recent error seen starting the sled-agent, if any:
    last_start_error_seen TEXT,

    -- Only allow IPv6 INET values for sled_subnet:
    CONSTRAINT sled_subnet_family CHECK (
        family(sled_subnet) = 6
    ),
    -- Enforce the correct length of the sled subnet:
    CONSTRAINT sled_subnet_prefix CHECK (
        masklen(sled_subnet) = 64
    ),

    -- Once the sled-agent starts successfully, we no longer want to track the
    -- last error seen:
    CONSTRAINT start_error_is_null_if_started CHECK (
        (NOT sled_agent_started) OR (last_start_error_seen IS NULL)
    ),

    PRIMARY KEY (inv_collection_id, sled_id)
);
