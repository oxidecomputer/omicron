SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.audit_log SET auth_method = auth_method_temp;
