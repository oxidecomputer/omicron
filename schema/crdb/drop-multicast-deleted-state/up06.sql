SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.multicast_group
    SET state_temp = CASE state::text
        WHEN 'creating' THEN 'creating'::omicron.public.multicast_group_state_temp
        WHEN 'active' THEN 'active'::omicron.public.multicast_group_state_temp
        WHEN 'deleting' THEN 'deleting'::omicron.public.multicast_group_state_temp
    END
    WHERE state_temp IS NULL;
