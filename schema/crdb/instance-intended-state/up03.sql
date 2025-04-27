-- backfill instance intended states based on the current state of the instance.
--
-- this is a bit conservative: we assume the intended state is running if and
-- only if the instance is currently running, because we don't know what the
-- user's desired state was otherwise.
SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.instance SET intended_state = CASE
    WHEN state = 'destroyed' THEN 'destroyed'
    WHEN state = 'vmm' THEN 'running'
    ELSE 'stopped'
END;
