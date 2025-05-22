CREATE INDEX IF NOT EXISTS lookup_attempts_for_webhook_delivery
ON omicron.public.webhook_delivery_attempt (
    delivery_id
);
