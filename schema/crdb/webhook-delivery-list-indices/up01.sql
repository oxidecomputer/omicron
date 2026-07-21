CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_by_time_created
ON omicron.public.webhook_delivery (
    time_created, id
);
