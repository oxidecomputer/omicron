CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery_attempt (
    -- Primary key
    id UUID PRIMARY KEY,
    -- Foreign key into `omicron.public.webhook_delivery`.
    delivery_id UUID NOT NULL,
    -- attempt number.
    attempt INT2 NOT NULL,

    -- UUID of the webhook receiver (foreign key into
    -- `omicron.public.webhook_rx`)
    rx_id UUID NOT NULL,

    result omicron.public.webhook_delivery_attempt_result NOT NULL,

    -- This is an INT4 to ensure we can store any unsigned 16-bit number,
    -- although status code > 599 would be Very Surprising...
    response_status INT4,
    response_duration INTERVAL,
    time_created TIMESTAMPTZ NOT NULL,
    -- UUID of the Nexus who did this delivery attempt.
    deliverator_id UUID NOT NULL,

    -- Attempt numbers start at 1
    CONSTRAINT attempts_start_at_1 CHECK (attempt >= 1),

    -- Ensure response status codes are not negative.
    -- We could be more prescriptive here, and also check that they're >= 100
    -- and <= 599, but some servers may return weird stuff, and we'd like to be
    -- able to record that they did that.
    CONSTRAINT response_status_is_unsigned CHECK (
        (response_status IS NOT NULL AND response_status >= 0) OR
            (response_status IS NULL)
    ),

    CONSTRAINT response_iff_not_unreachable CHECK (
        (
            -- If the result is 'succeedeed' or 'failed_http_error', response
            -- data must be present.
            (result = 'succeeded' OR result = 'failed_http_error') AND (
                response_status IS NOT NULL AND
                response_duration IS NOT NULL
            )
        ) OR (
            -- If the result is 'failed_unreachable' or 'failed_timeout', no
            -- response data is present.
            (result = 'failed_unreachable' OR result = 'failed_timeout') AND (
                response_status IS NULL AND
                response_duration IS NULL
            )
        )
    )
);
