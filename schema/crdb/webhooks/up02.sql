CREATE UNIQUE INDEX IF NOT EXISTS lookup_webhook_rxs_by_id
ON omicron.public.webhook_receiver (id)
WHERE
    time_deleted IS NULL;
