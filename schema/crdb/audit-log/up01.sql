CREATE TABLE IF NOT EXISTS omicron.public.audit_log (
    id UUID PRIMARY KEY,
    time_started TIMESTAMPTZ NOT NULL,
    -- request IDs are UUIDs but let's give them a little extra space
    -- https://github.com/oxidecomputer/dropshot/blob/83f78e7/dropshot/src/server.rs#L743
    request_id STRING(63) NOT NULL,
    request_uri STRING NOT NULL,
    operation_id STRING(512) NOT NULL,
    source_ip INET NOT NULL,
    -- Pulled from request header if present and truncated
    user_agent STRING(256),

    -- these three are all null if the request is unauthenticated. actor_id can
    -- be present while silo ID is null if the user is built in (non-silo).
    actor_id UUID,
    actor_silo_id UUID,
    -- The name of the authn scheme used
    access_method STRING,

    -- below are fields we can only fill in after the operation

    time_completed TIMESTAMPTZ,
    http_status_code INT4,

    -- only present on errors
    error_code STRING,
    error_message STRING
);
