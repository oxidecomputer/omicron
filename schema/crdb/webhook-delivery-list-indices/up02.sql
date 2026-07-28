CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_by_state
ON omicron.public.webhook_delivery (
    state
);
