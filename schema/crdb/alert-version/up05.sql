ALTER TABLE
    omicron.public.fm_alert_request
ADD CONSTRAINT IF NOT EXISTS
    alert_version_is_non_negative
CHECK (
    (alert_version >= 0)
);
