CREATE TABLE IF NOT EXISTS omicron.public.webhook_msg_delivery_attempt (
    id UUID PRIMARY KEY,
    -- Foreign key into `omicron.public.webhook_msg_dispatch`.
    dispatch_id UUID NOT NULL,
    result omicron.public.webhook_msg_delivery_result NOT NULL,
    response_status INT2,
    response_duration INTERVAL,
    time_created TIMESTAMPTZ NOT NULL,

    CONSTRAINT response_iff_not_unreachable CHECK (
        (
            -- If the result is 'succeedeed' or 'failed_http_error', response
            -- data must be present.
            (result = 'succeeded' OR result = 'failed_http_error') AND (
                response_status IS NOT NULL AND
                response_duration IS NOT NULL
            )
        ) OR (
            -- If the result is 'failed_unreachable', no response data is
            -- present.
            (result = 'failed_unreachable') AND (
                response_status IS NULL AND
                response_duration IS NULL
            )
        )
    )
);
