CREATE TABLE IF NOT EXISTS omicron.public.webhook_receiver (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Child resource generation for secrets.
    secret_gen INT NOT NULL,

    -- Child resource generation for subscriptions. This is separate from
    -- `secret_gen`, as updating secrets and updating subscriptions are separate
    -- operations which don't conflict with each other.
    subscription_gen INT NOT NULL,
    -- URL of the endpoint webhooks are delivered to.
    endpoint STRING(512) NOT NULL
);
