ALTER TABLE
    omicron.public.alert
ADD CONSTRAINT IF NOT EXISTS
    alert_version_is_non_negative
CHECK (
    (alert_version >= 0)
);
