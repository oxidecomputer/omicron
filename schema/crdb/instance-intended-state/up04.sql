-- backfill instances which don't have active VMM records.
--
-- this has to be done separately from `up04.sql` as the `FROM
-- omicron.public.vmm` clause to join with the active VMM record will not update
-- instances without active VMM records.
SET LOCAL disallow_full_table_scans = off;
UPDATE instance SET intended_state = CASE
    WHEN instance.state = 'destroyed' THEN 'destroyed'
    WHEN instance.state = 'failed' THEN 'running'
    ELSE 'stopped'
END
-- only update instances whose states were not touched in the previous step.
WHERE instance.intended_state IS NULL;
