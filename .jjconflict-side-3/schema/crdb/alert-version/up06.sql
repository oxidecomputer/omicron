-- Now that all existing alert requests have been backfilled to version 0, drop
-- the default so that all newly-inserted alert requests must specify a version
-- explicitly.
ALTER TABLE omicron.public.fm_alert_request
    ALTER COLUMN alert_version DROP DEFAULT;
