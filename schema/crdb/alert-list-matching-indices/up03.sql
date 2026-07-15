CREATE INDEX IF NOT EXISTS lookup_alerts_by_time_created
ON omicron.public.alert (
    time_created
);
