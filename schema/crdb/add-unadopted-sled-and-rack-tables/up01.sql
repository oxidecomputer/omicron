-- Tracks other racks that have joined the underlay network but haven't been
-- adopted into the multirack cluster.
--
-- If/when a rack is adopted, its row in this table should be marked deleted,
-- and a corresponding row should be inserted into the `rack` table.
CREATE TABLE IF NOT EXISTS omicron.public.unadopted_rack (
    -- This should be the same id that gets used for the `rack` table, as well
    -- as the `rack_id` column in `unadopted_sled`:
    id UUID PRIMARY KEY,

    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,

    -- The IPv6 underlay /56 prefix for the rack:
    rack_subnet INET NOT NULL,

    -- Underlay address of the particular sled-agent instance on the unadopted
    -- rack that's running the multirack join service:
    join_service_ip INET NOT NULL,
)
