-- Seed the rendezvous table from the current target blueprint so existing
-- deployments don't see an empty table (zero sleds available for provisioning)
-- between this migration and the first blueprint rendezvous run.
SET LOCAL disallow_full_table_scans = off;
INSERT INTO omicron.public.rendezvous_sled_bp_availability (
    sled_id,
    bp_availability,
    update_disposition_generation,
    blueprint_id,
    time_created,
    time_modified
)
SELECT
    m.sled_id,
    (CASE
        WHEN m.sled_state = 'decommissioned' THEN 'decommissioned'
        WHEN m.update_availability = 'available' THEN 'available'
        ELSE 'unavailable'
    END)::omicron.public.sled_bp_availability,
    CASE
        WHEN m.sled_state = 'decommissioned' THEN NULL
        ELSE m.update_disposition_generation
    END,
    m.blueprint_id,
    now(),
    now()
FROM omicron.public.bp_sled_metadata AS m
WHERE m.blueprint_id = (
    SELECT blueprint_id
    FROM omicron.public.bp_target
    ORDER BY version DESC
    LIMIT 1
)
ON CONFLICT (sled_id) DO NOTHING;
