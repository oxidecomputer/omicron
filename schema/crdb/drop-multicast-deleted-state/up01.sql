SET LOCAL disallow_full_table_scans = off;
-- No code path has ever written 'deleted': groups are hard-deleted via
-- 'Deleting' in the same reconciler pass that tears down the dataplane.
DELETE FROM omicron.public.multicast_group_member
    WHERE external_group_id IN (
        SELECT id FROM omicron.public.multicast_group
        WHERE state::text = 'deleted'
    );
DELETE FROM omicron.public.underlay_multicast_group
    WHERE id IN (
        SELECT underlay_group_id FROM omicron.public.multicast_group
        WHERE state::text = 'deleted'
    );
DELETE FROM omicron.public.multicast_group
    WHERE state::text = 'deleted';
