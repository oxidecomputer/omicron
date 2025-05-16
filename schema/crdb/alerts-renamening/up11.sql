CREATE INDEX IF NOT EXISTS lookup_alert_rxs_for_class
ON omicron.public.alert_subscription (
    alert_class
);
