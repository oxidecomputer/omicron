SET LOCAL disallow_full_table_scans = off;

-- Backfill any VMMs that are already in the 'failed' state with a
-- 'prehistoric' failure reason.
UPDATE omicron.public.vmm
    SET failure_reason = 'prehistoric'
    WHERE state = 'failed' AND failure_reason IS NULL;
