SET LOCAL disallow_full_table_scans = off;

-- Backfill the `slot` column for all ereport rows in a single pass:
UPDATE omicron.public.ereport
    SET slot = CASE ereport.reporter
        -- For SP reporters, the slot is simply the existing `sp_slot` value.
        WHEN 'sp' THEN ereport.sp_slot

        -- For host OS reporters, attempt to determine the slot number by
        -- looking up the sled in the most recent inventory collection that
        -- contains it, then joining through the baseboard to find the
        -- corresponding service processor's slot number. If the sled doesn't
        -- appear in any inventory collection, or the baseboard is unknown, the
        -- slot will remain NULL.
        WHEN 'host' THEN (
            SELECT sp.sp_slot
            FROM omicron.public.inv_sled_agent AS sled_agent
            JOIN omicron.public.inv_collection AS inv
                ON sled_agent.inv_collection_id = inv.id
            JOIN omicron.public.inv_service_processor AS sp
                ON sp.hw_baseboard_id = sled_agent.hw_baseboard_id
                AND sled_agent.inv_collection_id = sled_agent.inv_collection_id
            WHERE sled_agent.sled_id = ereport.sled_id
                AND sled_agent.hw_baseboard_id IS NOT NULL
            ORDER BY inv.time_done DESC
            LIMIT 1
        )
    END;
