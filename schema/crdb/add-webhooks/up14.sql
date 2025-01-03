CREATE INDEX IF NOT EXISTS lookup_webhook_delivery_attempt_for_msg
ON omicron.public.webhook_delivery_attempt (
    delivery_id
);
