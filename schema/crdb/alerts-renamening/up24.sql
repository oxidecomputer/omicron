-- Index for looking up all webhook messages dispatched to a receiver ID
CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_dispatched_to_rx
ON omicron.public.webhook_delivery (
    rx_id, alert_id
);
