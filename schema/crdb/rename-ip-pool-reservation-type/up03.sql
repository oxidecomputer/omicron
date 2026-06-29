SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.ip_pool
    SET assignment = CASE reservation_type
        WHEN 'external_silos' THEN 'silos'::omicron.public.ip_pool_assignment
        ELSE 'system_services'::omicron.public.ip_pool_assignment
    END;
