CREATE TYPE IF NOT EXISTS omicron.public.webhook_delivery_attempt_result as ENUM (
    -- The delivery attempt failed with an HTTP error.
    'failed_http_error',
    -- The delivery attempt failed because the receiver endpoint was
    -- unreachable.
    'failed_unreachable',
    --- The delivery attempt connected successfully but no response was received
    --  within the timeout.
    'failed_timeout',
    -- The delivery attempt succeeded.
    'succeeded'
);
