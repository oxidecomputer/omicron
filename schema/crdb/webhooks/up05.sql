CREATE INDEX IF NOT EXISTS lookup_webhook_secrets_by_rx
ON omicron.public.webhook_secret (
    rx_id
) WHERE
    time_deleted IS NULL;
