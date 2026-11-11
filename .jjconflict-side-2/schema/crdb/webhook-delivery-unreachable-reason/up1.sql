ALTER TABLE omicron.public.webhook_delivery_attempt
    ADD COLUMN IF NOT EXISTS unreachable_reason STRING;
