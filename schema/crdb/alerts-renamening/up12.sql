CREATE INDEX IF NOT EXISTS lookup_exact_subscriptions_for_alert_rx
on omicron.public.alert_subscription (
    rx_id
) WHERE glob IS NULL;
