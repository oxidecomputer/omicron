CREATE VIEW audit_log_complete AS
SELECT 
    id,
    timestamp,
    request_id,
    request_uri,
    operation_id,
    source_ip,
    actor_id,
    actor_silo_id,
    access_method,
    resource_id,
    time_completed,
    http_status_code,
    error_code,
    error_message
FROM audit_log
WHERE time_completed IS NOT NULL;
