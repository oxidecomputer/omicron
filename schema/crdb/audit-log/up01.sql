CREATE TABLE IF NOT EXISTS audit_log (
    id UUID NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    -- TODO: sizes on all strings
    request_id STRING NOT NULL,
    request_uri STRING NOT NULL,
    operation_id STRING NOT NULL,
    source_ip STRING NOT NULL,

    actor_id UUID,
    actor_silo_id UUID,
    access_method STRING,

    -- fields we can only fill in after the operation
    resource_id UUID,
    time_completed TIMESTAMPTZ,
    http_status_code INT4,
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
