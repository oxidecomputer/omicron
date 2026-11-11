SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.inv_svc_enabled_not_online_service
    SET state_temp = CASE state::text
        WHEN 'offline' THEN 'offline'::omicron.public.inv_svc_enabled_not_online_state_temp
        WHEN 'degraded' THEN 'degraded'::omicron.public.inv_svc_enabled_not_online_state_temp
        WHEN 'maintenance' THEN 'maintenance'::omicron.public.inv_svc_enabled_not_online_state_temp
    END
    WHERE state_temp IS NULL;
