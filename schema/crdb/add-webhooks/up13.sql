CREATE TABLE IF NOT EXISTS omicron.public.webhook_delivery_attempt (
    -- Foreign key into `omicron.public.webhook_delivery`.
    delivery_id UUID NOT NULL,
    -- attempt number.
    attempt INT2 NOT NULL,
    result omicron.public.webhook_delivery_result NOT NULL,
    -- A status code > 599 would be Very Surprising, so rather than using an
    -- INT4 to store a full unsigned 16-bit number in the database, we'll use a
    -- signed 16-bit integer with a check constraint that it's unsigned.
    response_status INT2,
    response_duration INTERVAL,
    time_created TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (delivery_id, attempt),

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
