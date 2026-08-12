SET LOCAL disallow_full_table_scans = off;

-- Backfill the ereporter_restart table from existing ereport data.
--
-- Insert one row for each unique restart ID present in the ereport table:
--
--   * `time_first_seen` is the earliest `time_collected` of any ereport with
--     that restart ID.
--   * `reporter` and `slot_type` are constant for a given restart ID (a restart
--     is a single reporter incarnation occupying a single physical slot), so we
--     read them directly from the group.
--   * The slot number may be NULL in some of a restart ID's ereports and
--     non-NULL in others: a sled host OS can emit ereports before its physical
--     location is known, and then emit more once it has been resolved. SQL's
--     `MAX` ignores NULLs, so `MAX(slot)` recovers the non-NULL slot if *any*
--     ereport for the restart ID had one. If every ereport's slot was NULL, the
--     restart's slot is left NULL, which the column permits.
--
-- `ON CONFLICT (id) DO NOTHING` makes this insert idempotent, and also tolerates
-- the (schema-prohibited, domain-impossible) case of a single restart ID having
-- inconsistent reporter/slot_type values across its ereports.
INSERT INTO omicron.public.ereporter_restart (
    id,
    time_first_seen,
    reporter,
    slot_type,
    slot
)
SELECT
    restart_id,
    MIN(time_collected),
    reporter,
    slot_type,
    MAX(slot)
FROM
    omicron.public.ereport
GROUP BY
    restart_id,
    reporter,
    slot_type
ON CONFLICT (id) DO NOTHING;
