CREATE TABLE IF NOT EXISTS omicron.public.audit_log (
    id UUID PRIMARY KEY,
    time_started TIMESTAMPTZ NOT NULL,
    -- request IDs are UUIDs but let's give them a little extra space
    -- https://github.com/oxidecomputer/dropshot/blob/83f78e7/dropshot/src/server.rs#L743
    request_id STRING(63) NOT NULL,
    request_uri STRING(512) NOT NULL,
    operation_id STRING(512) NOT NULL,
    source_ip INET NOT NULL,
    -- Pulled from request header if present and truncated
    user_agent STRING(256),

    -- these are all null if the request is unauthenticated. actor_id can
    -- be present while silo ID is null if the user is built in (non-silo).
    actor_id UUID,
    actor_silo_id UUID,
    -- actor kind indicating builtin user, silo user, or unauthenticated
    actor_kind omicron.public.audit_log_actor_kind NOT NULL,
    -- The name of the authn scheme used
    auth_method STRING(63),

    -- below are fields we can only fill in after the operation

    time_completed TIMESTAMPTZ,
    http_status_code INT4,

    -- only present on errors
    error_code STRING,
    error_message STRING,

    -- result kind indicating success, error, or timeout
    result_kind omicron.public.audit_log_result_kind,

    -- make sure time_completed and result_kind are either both null or both not
    CONSTRAINT time_completed_and_result_kind CHECK (
        (time_completed IS NULL AND result_kind IS NULL)
        OR (time_completed IS NOT NULL AND result_kind IS NOT NULL)
    ),

    -- Enforce consistency between result_kind and related fields:
    -- 'timeout': no HTTP status or error details
    -- 'success': requires HTTP status, no error details
    -- 'error': requires HTTP status and error message
    -- other/NULL: no HTTP status or error details
    CONSTRAINT result_kind_state_consistency CHECK (
        CASE result_kind
            WHEN 'timeout' THEN http_status_code IS NULL AND error_code IS NULL
                AND error_message IS NULL
            WHEN 'success' THEN error_code IS NULL AND error_message IS NULL AND
                http_status_code IS NOT NULL
            WHEN 'error' THEN http_status_code IS NOT NULL AND error_message IS
                NOT NULL
            ELSE http_status_code IS NULL AND error_code IS NULL AND error_message
                IS NULL
        END
    ),

    -- Ensure valid actor ID combinations
    -- Constraint: actor_kind and actor_id must be consistent
    CONSTRAINT actor_kind_and_id_consistent CHECK (
        -- For user_builtin: must have actor_id, must not have actor_silo_id
        (actor_kind = 'user_builtin' AND actor_id IS NOT NULL AND actor_silo_id IS NULL)
        OR
        -- For silo_user: must have both actor_id and actor_silo_id
        (actor_kind = 'silo_user' AND actor_id IS NOT NULL AND actor_silo_id IS NOT NULL)
        OR
        -- For unauthenticated: must not have actor_id or actor_silo_id
        (actor_kind = 'unauthenticated' AND actor_id IS NULL AND actor_silo_id IS NULL)
    )
);
