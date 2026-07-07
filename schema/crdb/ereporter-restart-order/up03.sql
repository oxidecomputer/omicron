SET LOCAL disallow_full_table_scans = off;

-- Backfill the ereporter_restart table from existing ereport data.
--
-- For each unique restart_id in the ereport table, determine the reporter
-- type, slot type, and slot number, and assign generation numbers within
-- each (reporter, slot_type, slot) location based on the earliest
-- time_collected for that restart_id.  This reconstructs the restart
-- ordering from the timestamps of the collected ereports.
--
-- Only ereports with a known slot number are included, as the
-- ereporter_restart table requires a non-NULL slot.
INSERT INTO omicron.public.ereporter_restart (
    id,
    generation,
    reporter_type,
    slot_type,
    slot,
    time_first_seen
)
SELECT
    restart_id,
    ROW_NUMBER() OVER (
        PARTITION BY reporter, slot_type, slot
        ORDER BY time_first_seen
    ) - 1 AS generation,
    reporter,
    slot_type,
    slot,
    time_first_seen
FROM (
    SELECT
        restart_id,
        reporter,
        slot_type,
        slot,
        MIN(time_collected) AS time_first_seen
    FROM
        omicron.public.ereport
    WHERE
        slot IS NOT NULL
    GROUP BY
        restart_id,
        reporter,
        slot_type,
        slot
) AS restarts
ON CONFLICT (id) DO NOTHING;
