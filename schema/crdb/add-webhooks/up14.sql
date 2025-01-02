CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_attempts_for_msg
ON omicron.public.webhook_delivery_attempts (
    delivery_id
);
