ALTER TABLE omicron.public.webhook_delivery_attempt
    ADD CONSTRAINT IF NOT EXISTS unreachable_reason_iff_unreachable CHECK (
        (result = 'failed_unreachable' AND unreachable_reason IS NOT NULL) OR
        (result != 'failed_unreachable' AND unreachable_reason IS NULL)
    );
