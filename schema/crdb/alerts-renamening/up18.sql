-- Describes the state of an alert delivery
CREATE TYPE IF NOT EXISTS omicron.public.alert_delivery_state AS ENUM (
    --  This delivery has not yet completed.
    'pending',
    -- This delivery has failed.
    'failed',
    --- This delivery has completed successfully.
    'delivered'
);
