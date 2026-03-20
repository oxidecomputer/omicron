SET LOCAL disallow_full_table_scans = 'off';

-- Backfill the new slot_type and slot columns for SP ereports from the
-- existing sp_type and sp_slot columns.
UPDATE omicron.public.ereport
SET slot_type = sp_type, slot = sp_slot
WHERE sp_type is NOT NULL
    AND slot_type IS NULL;
