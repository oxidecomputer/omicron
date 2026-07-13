CREATE INDEX IF NOT EXISTS lookup_alerts_by_class
ON omicron.public.alert (
    alert_class
);
