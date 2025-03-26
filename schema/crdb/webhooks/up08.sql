CREATE INDEX IF NOT EXISTS lookup_webhook_event_globs_for_rx
ON omicron.public.webhook_rx_event_glob (
    rx_id
);
