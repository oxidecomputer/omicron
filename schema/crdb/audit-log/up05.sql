CREATE VIEW IF NOT EXISTS omicron.public.audit_log_complete AS
SELECT
    id,
    time_started,
    request_id,
    request_uri,
    operation_id,
    source_ip,
    user_agent,
    actor_id,
    actor_silo_id,
    actor_kind,
    auth_method,
    time_completed,
    http_status_code,
    error_code,
    error_message,
    result_kind
FROM omicron.public.audit_log
WHERE
   time_completed IS NOT NULL
   AND result_kind IS NOT NULL;
