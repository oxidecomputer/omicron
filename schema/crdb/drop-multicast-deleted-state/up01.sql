SET LOCAL disallow_full_table_scans = off;
-- No code path has ever written 'deleted': groups are hard-deleted from
-- 'Deleting' in the same reconciler pass that tears down the dataplane.
-- This delete is defensive so the type swap below cannot strand a row.
DELETE FROM omicron.public.multicast_group
    WHERE state::text = 'deleted';
