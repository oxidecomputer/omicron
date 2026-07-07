-- Copy data from device_access_token to device_access_token_new
-- Full table scan is unavoidable when copying all data
SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.device_access_token_new
    (id, token, client_id, device_code, silo_user_id, time_requested, time_created, time_expires)
SELECT
    id, token, client_id, device_code, silo_user_id, time_requested, time_created, time_expires
FROM omicron.public.device_access_token
ON CONFLICT DO NOTHING;
