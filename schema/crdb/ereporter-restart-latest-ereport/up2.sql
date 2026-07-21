-- Backfill `time_latest_ereport_received` for ereporter restarts that were
-- recorded before this column existed.
SET LOCAL disallow_full_table_scans = off;

UPDATE
    omicron.public.ereporter_restart AS r
SET
    time_latest_ereport_received = COALESCE(
        -- If any ereports with this restart ID exist, take the max
        -- `time_collected` value from all those records.
        --
        -- Note that we include soft-deleted rows here on purpose. In normal
        -- operation, this timestamp is set when *inserting* new ereports, and
        -- it will not be changed when those ereports are soft-deleted.
        -- Including soft-deleted records here reflects the runtime behavior of
        -- the query that sets this timestamp.
        (
            SELECT
                max(e.time_collected)
            FROM
                omicron.public.ereport AS e
            WHERE
                e.restart_id = r.id
        ),
        -- If all ereports from a reporter have been *hard-deleted*, there isn't
        -- a lot we can do to reconstruct the time the latest ereport from that
        -- restart was received. The best heuristic we have here is that it
        -- can't possibly be earlier than `time_first_seen`, so use that.
        --
        -- Luckily, this is mostly academic, as we don't currently hard-delete
        -- ereports at all. :)
        r.time_first_seen
    )
WHERE
    time_latest_ereport_received IS NULL;
