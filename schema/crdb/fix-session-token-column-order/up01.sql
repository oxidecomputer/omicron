-- Create console_session_new with correct column order (id first)
CREATE TABLE IF NOT EXISTS omicron.public.console_session_new (
    id UUID PRIMARY KEY,
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);
