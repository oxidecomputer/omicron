SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.inv_svc_enabled_not_online_service
    SET state = CASE state_temp::text
        WHEN 'offline' THEN 'offline'::omicron.public.inv_svc_enabled_not_online_state
        WHEN 'degraded' THEN 'degraded'::omicron.public.inv_svc_enabled_not_online_state
        WHEN 'maintenance' THEN 'maintenance'::omicron.public.inv_svc_enabled_not_online_state
    END
    WHERE state IS NULL AND state_temp IS NOT NULL;
