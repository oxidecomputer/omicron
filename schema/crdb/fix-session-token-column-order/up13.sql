-- Rename the primary key constraint to match dbinit.sql
-- IF EXISTS makes this a no-op if the constraint was already renamed
ALTER INDEX IF EXISTS omicron.public.device_access_token_new_pkey
    RENAME TO device_access_token_pkey;
