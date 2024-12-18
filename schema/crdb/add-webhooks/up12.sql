CREATE TYPE IF NOT EXISTS omicron.public.webhook_msg_delivery_result as ENUM (
    -- The delivery attempt failed with an HTTP error.
    'failed_http_error',
    -- The delivery attempt failed because the receiver endpoint was
    -- unreachable.
    'failed_unreachable',
    -- The delivery attempt succeeded.
    'succeeded'
);
