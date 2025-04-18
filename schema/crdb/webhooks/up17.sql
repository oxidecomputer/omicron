-- Describes the state of a webhook delivery
CREATE TYPE IF NOT EXISTS omicron.public.webhook_delivery_state AS ENUM (
    --  This delivery has not yet completed.
    'pending',
    -- This delivery has failed.
    'failed',
    --- This delivery has completed successfully.
    'delivered'
);
