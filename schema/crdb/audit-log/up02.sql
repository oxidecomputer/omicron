CREATE UNIQUE INDEX IF NOT EXISTS audit_log_by_time_completed
    ON omicron.public.audit_log (time_completed, id)
    WHERE time_completed IS NOT NULL;
