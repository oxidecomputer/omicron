CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_by_trigger
ON omicron.public.webhook_delivery (
    triggered_by
);
