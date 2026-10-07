SET LOCAL disallow_full_table_scans = 'off';

UPDATE omicron.public.inv_sled_agent
    SET rack_id = (
        SELECT id FROM rack LIMIT 1
    );
