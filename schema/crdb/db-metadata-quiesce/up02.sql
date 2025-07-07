ALTER TABLE omicron.public.db_metadata
    ALTER COLUMN quiesce_started DROP DEFAULT,
    ALTER COLUMN quiesce_completed DROP DEFAULT;
