CREATE TABLE IF NOT EXISTS omicron.public.webhook_rx (
    id UUID PRIMARY KEY,
    -- A human-readable identifier for this webhook receiver.
    name STRING(63) NOT NULL,
    description STRING(512) NOT NULL,
    -- URL of the endpoint webhooks are delivered to.
    endpoint STRING(512) NOT NULL,
    -- Whether or not liveness probes are sent to this receiver.
    probes_enabled BOOL NOT NULL,
    -- TODO(eliza): how do we track which roles are assigned to a webhook?
    time_created TIMESTAMPTZ NOT NULL,
    time_deleted TIMESTAMPTZ
);
