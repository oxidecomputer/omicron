SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.multicast_group
    SET state = CASE state_temp::text
        WHEN 'creating' THEN 'creating'::omicron.public.multicast_group_state
        WHEN 'active' THEN 'active'::omicron.public.multicast_group_state
        WHEN 'deleting' THEN 'deleting'::omicron.public.multicast_group_state
    END
    WHERE state IS NULL AND state_temp IS NOT NULL;
