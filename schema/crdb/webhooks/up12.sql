CREATE INDEX IF NOT EXISTS lookup_exact_subscriptions_for_webhook_rx
on omicron.public.webhook_rx_subscription (
    rx_id
) WHERE glob IS NULL;
