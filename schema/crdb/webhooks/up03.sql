CREATE UNIQUE INDEX IF NOT EXISTS lookup_webhook_rx_by_name
ON omicron.public.webhook_receiver (
    name
) WHERE
    time_deleted IS NULL;
