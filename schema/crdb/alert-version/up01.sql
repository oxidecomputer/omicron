-- Add the `alert_version` column to the `alert` table. Existing alerts are
-- backfilled to version 0, as there are no other versions yet.
ALTER TABLE
    omicron.public.alert
ADD COLUMN IF NOT EXISTS
    alert_version
INT8 NOT NULL DEFAULT 0;
