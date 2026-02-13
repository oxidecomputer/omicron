-- Copy data from console_session to console_session_new
-- Full table scan is unavoidable when copying all data
SET LOCAL disallow_full_table_scans = off;

INSERT INTO omicron.public.console_session_new
    (id, token, time_created, time_last_used, silo_user_id)
SELECT
    id, token, time_created, time_last_used, silo_user_id
FROM omicron.public.console_session
ON CONFLICT DO NOTHING;
