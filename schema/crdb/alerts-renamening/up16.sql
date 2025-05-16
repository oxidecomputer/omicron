CREATE INDEX IF NOT EXISTS lookup_undispatched_alerts
ON omicron.public.alert (
    id, time_created
) WHERE time_dispatched IS NULL;
