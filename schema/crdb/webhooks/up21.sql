CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_for_event
ON omicron.public.webhook_delivery (
    event_id
);
