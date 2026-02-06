-- Create console_session_new with correct column order (id first)
-- Use explicit constraint name so it matches dbinit.sql after rename
CREATE TABLE IF NOT EXISTS omicron.public.console_session_new (
    id UUID,
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL,
    CONSTRAINT console_session_pkey PRIMARY KEY (id)
);
