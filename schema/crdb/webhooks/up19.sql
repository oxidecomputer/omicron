CREATE UNIQUE INDEX IF NOT EXISTS one_webhook_event_dispatch_per_rx
ON omicron.public.webhook_delivery (
    event_id, rx_id
)
WHERE
    triggered_by = 'event';
