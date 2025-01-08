CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx (
    /* Identity metadata (resource) */
    id UUID PRIMARY KEY,
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_modified TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ,
    -- Child resource generation
    rcgen INT NOT NULL,
    -- URL of the endpoint webhooks are delivered to.
    endpoint STRING(512) NOT NULL,
    -- Whether or not liveness probes are sent to this receiver.
    probes_enabled BOOL NOT NULL
);
