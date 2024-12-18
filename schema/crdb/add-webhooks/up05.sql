CREATE INDEX IF NOT EXISTS lookup_webhook_subscriptions_by_rx
ON omicron.public.webhook_rx_subscription (
    rx_id
);
