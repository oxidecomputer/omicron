SET LOCAL disallow_full_table_scans = 'off';

-- Backfill the new slot_type and slot columns for host OS ereports from
-- inventory data.
--
-- The join chain is:
--   ereport.sled_id -> inv_sled_agent.sled_id
--     -> inv_sled_agent.hw_baseboard_id -> inv_service_processor.hw_baseboard_id
--
-- We use DISTINCT ON to get the most recent inventory record for each sled.
-- The subquery aliases sp_type/sp_slot to avoid ambiguity with ereport's
-- own sp_type/sp_slot columns (which still exist at this point in the
-- migration).
UPDATE omicron.public.ereport AS e
SET
    slot_type = inv.slot_type,
    slot = inv.slot
FROM (
    SELECT DISTINCT ON (isa.sled_id)
        isa.sled_id,
        isp.sp_type AS slot_type,
        isp.sp_slot AS slot
    FROM omicron.public.inv_sled_agent AS isa
    INNER JOIN omicron.public.inv_service_processor AS isp
        ON isa.hw_baseboard_id = isp.hw_baseboard_id
        AND isa.inv_collection_id = isp.inv_collection_id
    WHERE isa.hw_baseboard_id IS NOT NULL
    ORDER BY isa.sled_id, isa.inv_collection_id DESC
) AS inv
WHERE e.reporter = 'host'
    AND e.sled_id = inv.sled_id
    AND e.slot_type IS NULL;
