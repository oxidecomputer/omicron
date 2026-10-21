-- Tracks sleds from other racks that have joined the underlay network but
-- haven't been adopted into the multirack cluster.
--
-- If/when these sleds are adopted, these rows should be marked deleted, and
-- there will subsequently be corresponding rows created in the `sled` and
-- `inv_sled_agent` tables via the normal mechanisms.
CREATE TABLE IF NOT EXISTS omicron.public.unadopted_sled (
    id UUID PRIMARY KEY,
    rack_id UUID NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- Baseboard information about the sled
    serial_number STRING(63) NOT NULL,
    part_number STRING(63) NOT NULL,

    ip INET NOT NULL
)
