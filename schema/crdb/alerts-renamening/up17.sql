CREATE TYPE IF NOT EXISTS omicron.public.alert_delivery_trigger AS ENUM (
    --  This delivery was triggered by the alert being dispatched.
    'alert',
    -- This delivery was triggered by an explicit call to the alert resend API.
    'resend',
    --- This delivery is a liveness probe.
    'probe'
);
