CREATE INDEX IF NOT EXISTS lookup_alerts_by_time_dispatched
ON omicron.public.alert (
    time_dispatched,
    time_created
);
