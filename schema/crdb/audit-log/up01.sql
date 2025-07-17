CREATE TABLE IF NOT EXISTS audit_log (
    id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    -- request IDs are UUIDs but let's give them a little extra space
    -- https://github.com/oxidecomputer/dropshot/blob/83f78e7/dropshot/src/server.rs#L743
    request_id STRING(63) NOT NULL,
    request_uri STRING NOT NULL,
    operation_id STRING(512) NOT NULL,
    source_ip INET NOT NULL,
    -- Pulled from request header if present
    user_agent STRING(512),

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
    error_message STRING,

    -- this stuff avoids table scans when filtering and sorting by timestamp
    -- sequential field must go after the random field
    -- https://www.cockroachlabs.com/docs/v22.1/performance-best-practices-overview#use-multi-column-primary-keys
    -- https://www.cockroachlabs.com/docs/v22.1/hash-sharded-indexes#create-a-table-with-a-hash-sharded-secondary-index
    PRIMARY KEY (id, timestamp),
    INDEX (timestamp) USING HASH,
    INDEX (time_completed) USING HASH
);
