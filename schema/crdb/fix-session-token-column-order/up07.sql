-- Index for removing sessions for a user that's being deleted
CREATE INDEX IF NOT EXISTS lookup_console_by_silo_user
    ON omicron.public.console_session (silo_user_id);
