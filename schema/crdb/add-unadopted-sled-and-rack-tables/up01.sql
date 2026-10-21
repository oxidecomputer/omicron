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

    -- Socket address of the multirack join service running on a sled-agent in
    -- the unadopted rack:
    join_service_ip INET NOT NULL,
    join_service_port INT4
        CHECK (join_service_port BETWEEN 0 AND 65535)
        NOT NULL
)
