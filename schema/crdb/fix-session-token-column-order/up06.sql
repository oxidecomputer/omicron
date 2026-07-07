-- Index for cleaning up old tokens
CREATE INDEX IF NOT EXISTS lookup_console_by_creation
    ON omicron.public.console_session (time_created);
