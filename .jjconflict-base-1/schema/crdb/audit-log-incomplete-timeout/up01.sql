-- Supports "find stale incomplete rows ordered by time_started".
CREATE INDEX IF NOT EXISTS audit_log_incomplete_by_time_started
    ON omicron.public.audit_log (time_started, id)
    WHERE time_completed IS NULL;
