CREATE TYPE IF NOT EXISTS omicron.public.webhook_delivery_trigger AS ENUM (
    --  This delivery was triggered by the event being dispatched.
    'event',
    -- This delivery was triggered by an explicit call to the webhook event
    -- resend API.
    'resend',
    --- This delivery is a liveness probe.
    'probe'
);
