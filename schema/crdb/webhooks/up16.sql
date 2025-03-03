CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery (
    -- UUID of this delivery.
    id UUID PRIMARY KEY,
    --- UUID of the event (foreign key into `omicron.public.webhook_event`).
    event_id UUID NOT NULL,
    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,

    trigger omicron.public.webhook_delivery_trigger NOT NULL,

    payload JSONB NOT NULL,

    --- Delivery attempt count. Starts at 0.
    attempts INT2 NOT NULL,

    time_created TIMESTAMPTZ NOT NULL,
    -- If this is set, then this webhook message has either been delivered
    -- successfully, or is considered permanently failed.
    time_completed TIMESTAMPTZ,
    -- If true, this webhook delivery has failed permanently and is eligible to
    -- be resent.
    failed_permanently BOOLEAN NOT NULL,
    -- Deliverator coordination bits
    deliverator_id UUID,
    time_delivery_started TIMESTAMPTZ,

    CONSTRAINT attempts_is_non_negative CHECK (attempts >= 0),
    CONSTRAINT active_deliveries_have_started_timestamps CHECK (
        (deliverator_id IS NULL) OR (
            (deliverator_id IS NOT NULL) AND (time_delivery_started IS NOT NULL)
        )
    ),
    CONSTRAINT failed_permanently_only_if_completed CHECK (
        (failed_permanently IS false) OR (failed_permanently AND (time_completed IS NOT NULL))
    )
);
