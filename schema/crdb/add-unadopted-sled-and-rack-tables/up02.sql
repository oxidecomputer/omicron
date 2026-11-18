-- Tracks sleds from other racks that have joined the underlay network but
-- haven't been adopted into the multirack cluster.
--
-- If/when these sleds are adopted, these rows should be marked deleted, and
-- there will subsequently be corresponding rows created in the `sled` and
-- `inv_sled_agent` tables via the normal mechanisms.
CREATE TABLE IF NOT EXISTS omicron.public.unadopted_sled (
    id UUID PRIMARY KEY,
    -- Foreign key into the `unadopted_rack` table (though, also, the rack ID
    -- that gets used in the `rack` table should be the same if/when the rack
    -- is adopted)
    rack_id UUID NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- Baseboard information about the sled
    serial_number STRING(63) NOT NULL,
    part_number STRING(63) NOT NULL,

    sled_agent_ip INET NOT NULL,

    -- This tracks whether this particular sled's sled-agent has been
    -- successfully started by the multirack join service:
    sled_agent_started BOOL NOT NULL,
    -- This nullable column should be set to the last error we've seen for this
    -- particular sled's sled-agent startup process, if any:
    last_start_error_seen TEXT,

    CONSTRAINT start_error_is_null_if_started CHECK (
        (NOT sled_agent_started) OR (last_start_error_seen IS NULL)
    )
);
