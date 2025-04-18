CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_attempts_to_rx
ON omicron.public.webhook_delivery_attempt (
    rx_id
);
