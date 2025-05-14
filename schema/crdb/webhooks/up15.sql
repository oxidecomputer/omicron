CREATE INDEX IF NOT EXISTS lookup_undispatched_webhook_events
ON omicron.public.webhook_event (
    id, time_created
) WHERE time_dispatched IS NULL;
