-- Add the `alert_version` column to the `fm_alert_request` table. Existing alert
-- requests are backfilled to version 0, as there are no other versions yet.
ALTER TABLE
    omicron.public.fm_alert_request
ADD COLUMN IF NOT EXISTS
    alert_version
INT8 NOT NULL DEFAULT 0;
