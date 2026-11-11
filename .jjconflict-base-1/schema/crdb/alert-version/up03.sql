-- Now that all existing alerts have been backfilled to version 0, drop the
-- default so that all newly-inserted alerts must specify a version explicitly.
ALTER TABLE
    omicron.public.alert
ALTER COLUMN
    alert_version
DROP DEFAULT;
