-- Index for looking up all delivery attempts for an alert
CREATE INDEX IF NOT EXISTS lookup_webhook_deliveries_for_alert
ON omicron.public.webhook_delivery (
    alert_id
);
