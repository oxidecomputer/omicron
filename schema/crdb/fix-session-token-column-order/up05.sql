-- Rename the primary key constraint to match dbinit.sql
-- IF EXISTS makes this a no-op if the constraint was already renamed
ALTER INDEX IF EXISTS omicron.public.console_session_new_pkey
    RENAME TO console_session_pkey;
