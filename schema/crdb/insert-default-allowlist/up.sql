-- This is a one-time insertion of the default allowlist for user-facing
-- services on existing racks.
--
-- During RSS, this row is populated by the bootstrap agent. Nexus awaits its
-- presence before launching its external server, to ensure the list is active
-- from the first request Nexus serves.
--
-- However, on existing racks, this row doesn't exist, and RSS also doesn't run.
-- Thus Nexus waits forever. Insert the default now, ignoring any conflict with
-- an existing row.
INSERT INTO omicron.public.allow_list (id, time_created, time_modified, allowed_ips)
VALUES (
    -- Hardcoded ID, see nexus/db-queries/src/db/fixed_data/allow_list.rs.
    '001de000-a110-4000-8000-000000000000',
    NOW(),
    NOW(),
    -- No allowlist at all, meaning allow any external traffic.
    NULL
)
-- If the row already exists, RSS has already run and the bootstrap agent has
-- inserted this record. Do not overwrite it.
ON CONFLICT (id)
DO NOTHING;
