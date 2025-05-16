-- Delivery dispatch table for webhook receivers.
CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery (
    -- UUID of this delivery.
    id UUID PRIMARY KEY,
    --- UUID of the alert (foreign key into `omicron.public.alert`).
    alert_id UUID NOT NULL,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.alert_receiver`)
    rx_id UUID NOT NULL,

    triggered_by omicron.public.alert_delivery_trigger NOT NULL,

    --- Delivery attempt count. Starts at 0.
    attempts INT2 NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,
    -- If this is set, then this webhook message has either been delivered
    -- successfully, or is considered permanently failed.
    time_completed TIMESTAMPTZ,

    state omicron.public.alert_delivery_state NOT NULL,

    -- Deliverator coordination bits
    deliverator_id UUID,
    time_leased TIMESTAMPTZ,

    CONSTRAINT attempts_is_non_negative CHECK (attempts >= 0),
    CONSTRAINT active_deliveries_have_started_timestamps CHECK (
        (deliverator_id IS NULL) OR (
            deliverator_id IS NOT NULL AND time_leased IS NOT NULL
        )
    ),
    CONSTRAINT time_completed_iff_not_pending CHECK (
        (state = 'pending' AND time_completed IS NULL) OR
            (state != 'pending' AND time_completed IS NOT NULL)
    )
);
