SET LOCAL disallow_full_table_scans = off;

DELETE FROM omicron.public.inv_svc_enabled_not_online_service
    WHERE state::text = 'uninitialized';
