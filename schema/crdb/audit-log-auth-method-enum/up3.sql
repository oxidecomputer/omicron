SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.audit_log SET auth_method_temp = CASE
    WHEN auth_method = 'token' THEN 'access_token'
    WHEN auth_method = 'session_cookie' THEN 'session_cookie'
    WHEN auth_method = 'scim_token' THEN 'scim_token'
    WHEN auth_method = 'spoof' THEN 'spoof'
    ELSE NULL
END::omicron.public.audit_log_auth_method
WHERE auth_method IS NOT NULL;
