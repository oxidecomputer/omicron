
ALTER TABLE
    omicron.public.ereporter_restart
ADD COLUMN IF NOT EXISTS
    time_latest_ereport_received TIMESTAMPTZ;
